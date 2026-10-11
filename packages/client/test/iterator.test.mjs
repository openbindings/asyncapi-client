import assert from 'node:assert/strict';
import test from 'node:test';
import { HostSession, AsyncApiRuntimeError } from '../dist/index.js';

// Deterministic facade scheduling controls. The actual-host suites separately
// exercise the Rust receive path, peer traffic, cancellation and close events.
function controlledSession() {
  const session=new HostSession({free(){}}),calls=[];
  session.next=({signal})=>new Promise((resolve,reject)=>{
    const abort=()=>reject(new AsyncApiRuntimeError({code:'Cancelled',detail:'test cancellation',delivery_unknown:false}));
    signal.addEventListener('abort',abort,{once:true});
    calls.push({signal,resolve:value=>{signal.removeEventListener('abort',abort);resolve(value);},reject:error=>{signal.removeEventListener('abort',abort);reject(error);}});
    if(signal.aborted)abort();
  });
  return {session,calls};
}

test('iterator return cancels its pending read and later calls do not receive',async()=>{
  const {session,calls}=controlledSession(),cursor=session.incoming();
  const pending=cursor.next();await assert.rejects(cursor.next(),error=>error.code==='InvalidConfiguration');
  assert.deepEqual(await cursor.return(),{done:true,value:undefined});
  assert.deepEqual(await pending,{done:true,value:undefined});assert.equal(calls[0].signal.aborted,true);
  assert.equal((await cursor.next()).done,true);assert.equal(calls.length,1);session.dispose();
});

test('iterator return preserves an already admitted owning value for its pending caller',async()=>{
  const {session,calls}=controlledSession(),cursor=session.incoming();
  let freed=0;const value={dispose(){freed++;}},message={kind:'message',codec:'json',value,payload:new Uint8Array(),operation:0};
  const pending=cursor.next();calls[0].resolve(message);const closing=cursor.return();
  const item=await pending;assert.equal(item.done,false);assert.equal(item.value,message);assert.equal(freed,0);
  item.value.value.dispose();assert.equal(freed,1);await closing;assert.equal((await cursor.next()).done,true);session.dispose();
});

test('iterator external cancellation remains an error when return follows it',async()=>{
  const {session}=controlledSession(),controller=new AbortController(),cursor=session.incoming({signal:controller.signal});
  const pending=cursor.next();controller.abort();const ending=cursor.return();
  await Promise.all([assert.rejects(pending,error=>error.code==='Cancelled'),assert.rejects(ending,error=>error.code==='Cancelled')]);
  assert.equal((await cursor.next()).done,true);session.dispose();
});

test('iterator removes external abort listeners after delivery and failure',async()=>{
  const {session,calls}=controlledSession(),controller=new AbortController(),signal=controller.signal;
  const add=signal.addEventListener.bind(signal),remove=signal.removeEventListener.bind(signal),listeners=new Set();
  signal.addEventListener=(type,fn,options)=>{if(type==='abort')listeners.add(fn);add(type,fn,options);};
  signal.removeEventListener=(type,fn)=>{if(type==='abort')listeners.delete(fn);remove(type,fn);};
  const cursor=session.incoming({signal});const pending=cursor.next();assert.equal(listeners.size,1);
  calls[0].resolve({kind:'rejected',reason:'test',payloadBytes:0});assert.equal((await pending).done,false);assert.equal(listeners.size,0);
  const failed=cursor.next(),expected=new Error('transport failure');calls[1].reject(expected);
  await assert.rejects(failed,error=>error===expected);assert.equal(listeners.size,0);assert.equal((await cursor.next()).done,true);session.dispose();
});
