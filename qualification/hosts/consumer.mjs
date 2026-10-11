function check(condition,message) { if(!condition)throw new Error(message); }
async function failure(operation,code) {try {await operation();}catch(error){check(error.code===code,`expected ${code}, got ${error.code}: ${error}`);return {code:error.code,deliveryUnknown:error.deliveryUnknown};}throw new Error(`expected ${code} failure`);}
function payload(sequence,size) {const value=new Uint8Array(size);for(let offset=4;offset<size;offset++)value[offset]=(sequence+offset)%251;new DataView(value.buffer).setUint32(0,sequence);return value;}
function plansFor(client,source) {
 const document=client.parse(source),plans=[];
 try {for(const id of ['emit','listen']) {const operation=document.operation(id);try {const compiled=operation.compile();try {plans.push(compiled.prepare({role:'application'}));}finally {compiled.dispose();}}finally {operation.dispose();}}return plans;}
 finally {document.dispose();}
}
// Observe library handler ownership through host property assignment. This
// wrapper preserves the actual host's WebSocket methods and transport behavior.
function instrumentSockets() {
 const Original=globalThis.WebSocket;
 const sockets=[];
 function Wrapped(url,protocols) {
  const socket=protocols===undefined?new Original(url):new Original(url,protocols);
  const record={url,handlers:new Set(),closeCalls:0,events:[]};
  for(const type of ['open','error','close']) socket.addEventListener(type,event=>record.events.push({type,readyState:socket.readyState,code:event.code,wasClean:event.wasClean,message:event.message}));sockets.push(record);
  return new Proxy(socket,{get(target,key){if(key==='close')return (...args)=>{record.closeCalls++;return target.close(...args);};const value=Reflect.get(target,key,target);return typeof value==='function'?value.bind(target):value;},set(target,key,value){if(['onopen','onmessage','onerror','onclose'].includes(key)){if(value==null)record.handlers.delete(key);else record.handlers.add(key);}return Reflect.set(target,key,value,target);}});
 }
 Wrapped.prototype=Original.prototype;Object.setPrototypeOf(Wrapped,Original);globalThis.WebSocket=Wrapped;
 return {sockets,restore(){globalThis.WebSocket=Original;},snapshot(){return {created:sockets.length,handlers:sockets.reduce((n,s)=>n+s.handlers.size,0),closeCalls:sockets.reduce((n,s)=>n+s.closeCalls,0)};}};
}
export async function exerciseSessions(client,outerExchange,source) {
 const observations=[];const tracking=instrumentSockets();const plans=plansFor(client,source);
 try {
  const aborted=new AbortController();aborted.abort();
  observations.push({name:'pre-aborted open performs no socket construction',...(await failure(()=>client.openSession(plans,{signal:aborted.signal}),'Cancelled'))});check(tracking.sockets.length===0,'pre-aborted open constructed a socket');
  const session=await client.openSession(plans);const sender=session.sender();
  try {
   const notice=await session.next({signal:AbortSignal.timeout(3000)});check(notice.kind==='rejected' && notice.payloadBytes===29,'unsolicited text was not rejected with exact length');observations.push({name:'text rejection',bytes:notice.payloadBytes});
   await failure(()=>session.next({signal:aborted.signal}),'Cancelled');
   const cancel=new AbortController();const waiting=session.next({signal:cancel.signal});
   await failure(()=>session.next(),'InvalidConfiguration');cancel.abort();await failure(()=>waiting,'Cancelled');observations.push({name:'receive cancellation preserves session and enforces single waiter'});
   await failure(()=>sender.send(1,new Uint8Array()),'InvalidConfiguration');
   await failure(()=>sender.send(8,new Uint8Array()),'InvalidConfiguration');
   await failure(()=>sender.send(0,new Uint8Array(1024*1024+1)),'Backpressure');
   for(let sequence=0;sequence<8;sequence++) {
    const expected=payload(sequence,1024);const receipt=sender.send(0,expected);check(receipt.kind==='webSocketHostAccepted','receipt overstates host evidence');
    const actual=await session.next({signal:AbortSignal.timeout(3000)});check(actual.kind==='message' && actual.operation===1 && actual.payload.length===expected.length && actual.payload.every((v,i)=>v===expected[i]),'echo bytes or routing mismatch');
   }
   check(session.usage.messages===0 && session.usage.bytes===0,'consumed queue retained quota');
   const close=await session.close();check(close.wasClean===true && close.code===1000,'clean close receipt missing');observations.push({name:'facade binary exchange and clean close',messages:8,bytes:8192,close});
   await failure(()=>sender.send(0,new Uint8Array()),'Closed');
  } finally {sender.dispose();session.dispose();}
  check(tracking.snapshot().handlers===0,'closed session retained callbacks');
  const dropped=await client.openSession(plans);await dropped.next({signal:AbortSignal.timeout(3000)});const retained=dropped.sender();
  const waiting=dropped.next();dropped.dispose();await failure(()=>waiting,'Closed');await failure(()=>retained.send(0,new Uint8Array()),'Closed');retained.dispose();
  check(tracking.snapshot().handlers===0,'disposed session retained callbacks');observations.push({name:'dispose cancels pending receive and invalidates retained sender'});
  for(let cycle=0;cycle<100;cycle++) {
   const session=await client.openSession(plans);
   try {await session.next({signal:AbortSignal.timeout(3000)});const close=await session.close();check(close.wasClean && close.code===1000,`cycle ${cycle} close mismatch`);}finally {session.dispose();}
   check(tracking.snapshot().handlers===0,`cycle ${cycle} retained callbacks`);
  }
  observations.push({name:'one hundred awaited closes release every handler',cycles:100});
  for(let sequence=0;sequence<8;sequence++) {
   const expected=payload(sequence,1024);const actual=await outerExchange(source,expected);
   check(actual.length===expected.length && actual.every((v,i)=>v===expected[i]),'outer Rust/Wasm execution altered bytes');
  }
  observations.push({name:'direct outer Rust/Wasm binary exchanges',messages:8,bytes:8192});
  check(tracking.snapshot().handlers===0,'outer Rust session retained callbacks');
  return {status:'passed',observations,ownership:tracking.snapshot()};
 } catch(error) { throw new Error(JSON.stringify({failure:String(error),observations,sockets:tracking.sockets.map(({url,handlers,...record})=>({...record,handlers:handlers.size}))})); } finally {for(const plan of plans)plan.dispose();tracking.restore();}
}


export async function rawCloseControl(source) {
 const input=JSON.parse(source);const url=`ws://${input.servers.local.host}/events`;
 const events=[];
 await new Promise((resolve,reject)=>{
  const socket=new WebSocket(url);socket.binaryType='arraybuffer';
  const timer=setTimeout(()=>reject(new Error('raw close deadline')),3000);
  socket.addEventListener('open',()=>{events.push({type:'open'});socket.close(1000);});
  socket.addEventListener('error',event=>events.push({type:'error',message:event.message,readyState:socket.readyState}));
  socket.addEventListener('close',event=>{events.push({type:'close',code:event.code,wasClean:event.wasClean});clearTimeout(timer);resolve();});
 });
 return events;
}
