function check(value,message){if(!value)throw new Error(message);}
async function failure(operation,code){try{await operation();}catch(error){check(error.code===code,`expected ${code}, got ${error.code}: ${error}`);return error.code;}throw new Error(`expected ${code}`);}
export async function exerciseFaults(client,source,port){
 const observations=[];
 // Security refusal is a preflight result in both actual hosts. The endpoint
 // deliberately cannot complete TLS with this plaintext fixture server.
 const secure=JSON.parse(source);secure.servers.local.protocol='wss';secure.servers.local.security=[{type:'X509'}];
 const authDoc=client.parse(JSON.stringify(secure)),authOp=authDoc.operation('emit'),authCompiled=authOp.compile(),authPlan=authCompiled.prepare({role:'application'});
 try {await failure(()=>client.openSession([authPlan]),'Unsupported');}
 finally {authPlan.dispose();authCompiled.dispose();authOp.dispose();authDoc.dispose();}
 observations.push({name:'declared X509 refuses before host WebSocket construction'});
 function plans(path){const parsed=JSON.parse(source);parsed.servers.local.host=`127.0.0.1:${port}`;parsed.channels.events.address=path;const document=client.parse(JSON.stringify(parsed));try{return ['emit','listen'].map(id=>{const operation=document.operation(id);try{const compiled=operation.compile();try{return compiled.prepare({role:'application'});}finally{compiled.dispose();}}finally{operation.dispose();}});}finally{document.dispose();}}
 async function withSession(path,options,run){const attached=plans(path);let session,sender;try{session=await client.openSession(attached,options);sender=session.sender();return await run(session,sender);}finally{sender?.dispose();session?.dispose();for(const plan of attached)plan.dispose();}}
 for(const [path,limits,count,size] of [['/count',{maxMessages:2},2,1],['/bytes',{maxBufferedBytes:8,maxMessageBytes:8},1,5]]){
  await withSession(path,{limits},async(session,sender)=>{sender.send(0,new Uint8Array([1]));await new Promise(r=>setTimeout(r,50));
   for(let i=0;i<count;i++){const next=await session.next({signal:AbortSignal.timeout(1000)});check(next.kind==='message' && next.payload.length===size,'queue lost valid frame before overflow');}
   await failure(()=>session.next(),'Backpressure');check(session.usage.messages===0 && session.usage.bytes===0,'overflow retained queue quota');
  });observations.push({name:path==='/count'?'message count overflow':'bounded queue overflow',path,retainedMessages:count});
 }
 await withSession('/large',{limits:{maxBufferedBytes:8,maxMessageBytes:8}},async(session,sender)=>{sender.send(0,new Uint8Array([1]));await failure(()=>session.next({signal:AbortSignal.timeout(1000)}),'Backpressure');});observations.push({name:'oversized message rejected before Rust body admission'});
 await withSession('/text',{},async(session,sender)=>{sender.send(0,new Uint8Array([1]));const next=await session.next({signal:AbortSignal.timeout(1000)});check(next.kind==='rejected' && next.payloadBytes===6,'UTF-8 rejected length mismatch');await session.close();});observations.push({name:'UTF-8 text rejection byte count'});
 await withSession('/abnormal',{},async(session,sender)=>{sender.send(0,new Uint8Array([1]));await failure(()=>session.next({signal:AbortSignal.timeout(1000)}),'Connection');});observations.push({name:'abnormal peer close is a failure'});
 await withSession('/no-close',{closeTimeoutMs:50},async(session,sender)=>{sender.send(0,new Uint8Array([1]));await session.next({signal:AbortSignal.timeout(1000)});await failure(()=>session.close(),'Deadline');await failure(()=>sender.send(0,new Uint8Array([1])),'Closed');});observations.push({name:'close deadline invalidates retained senders'});
 await withSession('/no-close',{closeTimeoutMs:2000},async(session,sender)=>{sender.send(0,new Uint8Array([1]));await session.next({signal:AbortSignal.timeout(1000)});const controller=new AbortController();const closing=session.close({signal:controller.signal});controller.abort();await failure(()=>closing,'Cancelled');await failure(()=>sender.send(0,new Uint8Array([1])),'Closed');});observations.push({name:'cancelled close releases its owner'});
 const attached=plans('/stall');try{
  await failure(()=>client.openSession(attached,{connectTimeoutMs:50}),'Deadline');
  const controller=new AbortController();const opening=client.openSession(attached,{signal:controller.signal});setTimeout(()=>controller.abort(),20);await failure(()=>opening,'Cancelled');
 }finally{for(const plan of attached)plan.dispose();}observations.push({name:'connect deadline and active cancellation release owner'});
 async function echo(session,sender,byte) {
  sender.send(0,new Uint8Array([byte]));
  const item=await session.next({signal:AbortSignal.timeout(1000)});
  check(item?.kind==='message' && item.payload.length===1 && item.payload[0]===byte,'stream cleanup lost the next echo');
 }
 await withSession('/stream-echo',{},async(session,sender)=>{
  sender.send(0,new Uint8Array([1]));
  for await(const item of session) {check(item.kind==='message' && item.payload[0]===1,'default iterator yielded wrong message');break;}
  await echo(session,sender,2);
  const expected=new Error('consumer body failed');let observed;
  sender.send(0,new Uint8Array([3]));
  try {for await(const item of session.incoming({signal:AbortSignal.timeout(1000)})) {check(item.kind==='message','consumer body received wrong kind');throw expected;}}
  catch(error){observed=error;}
  check(observed===expected,'iterator replaced the consumer exception');await echo(session,sender,4);await session.close();
 });observations.push({name:'iterator break and consumer exception preserve shared session'});
 await withSession('/stream-echo',{},async(session,sender)=>{
  for(const kind of ['return','asyncDispose']) {
   const iterator=session.incoming(),waiting=iterator.next();
   if(kind==='return')check((await iterator.return()).done,'iterator return did not finish');
   else await iterator[Symbol.asyncDispose]();
   check((await waiting).done && (await iterator.next()).done,'iterator retained a pending receive after exit');
   await echo(session,sender,kind==='return'?5:6);
  }
  const iterator=session.incoming(),waiting=iterator.next(),expected=new Error('explicit iterator throw');let observed;
  try {await iterator.throw(expected);}catch(error){observed=error;}
  check(observed===expected && (await waiting).done,'iterator throw lost its reason or pending waiter');
  await echo(session,sender,7);await session.close();
 });observations.push({name:'iterator return throw and async disposal interrupt pending receive'});
 await withSession('/stream-echo',{},async(session,sender)=>{
  const pre=new AbortController();pre.abort();const iterator=session.incoming({signal:pre.signal});
  await failure(()=>iterator.next(),'Cancelled');check((await iterator.next()).done,'pre-aborted cursor did not finish');await echo(session,sender,8);
  const active=new AbortController(),stream=session.incoming({signal:active.signal}),waiting=stream.next();active.abort();
  await failure(()=>waiting,'Cancelled');check((await stream.next()).done,'aborted cursor did not finish');await echo(session,sender,9);await session.close();
 });observations.push({name:'iterator pre-abort and active abort preserve session'});
 await withSession('/stream-echo',{},async(session,sender)=>{
  const iterator=session.incoming(),waiting=iterator.next();
  await failure(()=>iterator.next(),'InvalidConfiguration');await failure(()=>session.next(),'InvalidConfiguration');
  const other=session.incoming();await failure(()=>other.next(),'InvalidConfiguration');
  check((await other.next()).done,'failed second cursor remained active');await iterator.return();check((await waiting).done,'overlap replaced original waiter');
  await echo(session,sender,10);await session.close();
 });observations.push({name:'iterator overlapping receives refuse without replacing the first waiter'});
 await withSession('/stream-close',{},async(session,sender)=>{
  sender.send(0,new Uint8Array([1]));let count=0;
  for await(const item of session.incoming({signal:AbortSignal.timeout(1000)})) {check(item.kind==='message' && item.payload[0]===42,'clean end lost queued message');count++;}
  check(count===1,'clean end did not drain exactly one message');const receipt=await session.close();check(receipt.wasClean && receipt.code===1000,'remote close receipt changed');
 });observations.push({name:'iterator drains queued message before clean completion'});
 await withSession('/abnormal',{},async(session,sender)=>{
  const iterator=session.incoming({signal:AbortSignal.timeout(1000)}),waiting=iterator.next();sender.send(0,new Uint8Array([1]));
  await failure(()=>waiting,'Connection');check((await iterator.next()).done,'failed transport cursor remained active');
 });observations.push({name:'iterator preserves abnormal transport failure'});
 await withSession('/stream-echo',{},async(session,sender)=>{
  const iterator=session.incoming(),waiting=iterator.next();session.dispose();
  await failure(()=>waiting,'Closed');check((await iterator.next()).done,'disposed session retained cursor');await failure(()=>sender.send(0,new Uint8Array([1])),'Closed');
 });observations.push({name:'session disposal wakes pending iterator and invalidates sender'});
 return {status:'passed',observations};
}
