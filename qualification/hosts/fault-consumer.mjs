function check(value,message){if(!value)throw new Error(message);}
async function failure(operation,code){try{await operation();}catch(error){check(error.code===code,`expected ${code}, got ${error.code}: ${error}`);return error.code;}throw new Error(`expected ${code}`);}
export async function exerciseFaults(client,source,port){
 const observations=[];
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
 return {status:'passed',observations};
}
