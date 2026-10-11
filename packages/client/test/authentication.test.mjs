import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import {createClient, AsyncApiError, AsyncApiRuntimeError} from '../dist/index.js';
const client=await createClient({wasm:await readFile(new URL('../wasm/asyncapi_bg.wasm',import.meta.url))});
const source=()=>({asyncapi:'3.1.0',info:{title:'auth',version:'1'},servers:{s:{protocol:'wss',host:'localhost:1',security:[{type:'X509'},{$ref:'missing.json'}]}},channels:{c:{address:'/events',messages:{m:{contentType:'application/octet-stream'}}}},operations:{emit:{action:'send',channel:{$ref:'#/channels/c'}}}});

test('security choices and selected requirements use Rust semantics through the facade',()=>{
 const doc=client.parse(JSON.stringify(source()),{sourceUri:'https://fixture.test/api.json'}),op=doc.operation('emit'),compiled=op.compile();
 try {
  assert.throws(()=>compiled.authentication('s'),e=>e instanceof AsyncApiError && e.code==='MissingResource');
  assert.throws(()=>compiled.prepare({role:'application'}),e=>{
   assert.deepEqual(e.requirement,{kind:'authenticationChoice',scope:'server',choices:[0,1]});return true;
  });
  const plan=compiled.prepare({role:'application',security:{server:0}});
  try {assert.equal(plan.describe().authentication.server.schemes[0].schemeType,'X509');assert.equal(plan.describe().authentication.operation,null);}
  finally {plan.dispose();}
  for(const security of [{server:-1},{server:0.5},{server:2},{username:'secret-sentinel'}]) {
   assert.throws(()=>compiled.prepare({role:'application',security}),e=>e.code==='InvalidConfiguration' && !String(e).includes('secret-sentinel'));
  }
 }finally {compiled.dispose();op.dispose();doc.dispose();}
});

test('security inspection preserves legacy conjunction and component coordinates',()=>{
 const value={asyncapi:'2.6.0',info:{title:'auth',version:'1'},servers:{s:{protocol:'mqtts',protocolVersion:'3.1.1',url:'localhost:8883',security:[{user:[],cert:[]}]}},channels:{events:{subscribe:{operationId:'emit',message:{contentType:'application/octet-stream'}}}},components:{securitySchemes:{user:{type:'userPassword'},cert:{type:'X509'}}}};
 const doc=client.parse(JSON.stringify(value)),op=doc.operation('emit'),c=op.compile();
 try {
  const auth=c.authentication('s');assert.equal(auth.server[0].schemes.length,2);
  const user=auth.server[0].schemes.find(s=>s.componentName==='user');
  assert.equal(user.schemeType,'userPassword');assert.equal(user.definition.pointer,'/components/securitySchemes/user');
  assert.equal(user.selection.pointer,'/servers/s/security/0/user');assert.deepEqual(auth.operation,[]);
 }finally {c.dispose();op.dispose();doc.dispose();}
});

test('host refuses declared X509 before constructing a WebSocket',async()=>{
 const doc=client.parse(JSON.stringify(source())),op=doc.operation('emit'),c=op.compile(),plan=c.prepare({role:'application',security:{server:0}});
 const original=globalThis.WebSocket;let attempts=0;globalThis.WebSocket=class {constructor(){attempts++;throw Error('unexpected socket');}};
 try {await assert.rejects(client.openSession([plan]),e=>e instanceof AsyncApiRuntimeError && e.code==='Unsupported');assert.equal(attempts,0);}
 finally {globalThis.WebSocket=original;plan.dispose();c.dispose();op.dispose();doc.dispose();}
});

test('query key metadata and missing or unused credentials refuse before host I/O',async()=>{
 const value=source();value.servers.s.security=[{type:'httpApiKey',in:'query',name:'access key+雪'}];
 const doc=client.parse(JSON.stringify(value)),op=doc.operation('emit'),c=op.compile(),plan=c.prepare({role:'application'});
 const original=globalThis.WebSocket;let attempts=0;globalThis.WebSocket=class {constructor(){attempts++;throw Error('unexpected socket');}};
 try {
  assert.deepEqual(c.authentication('s').server[0].schemes[0].httpApiKey,{name:'access key+雪',location:'query'});
  for(const queryCredentials of [undefined,{'access key+雪':'secret-sentinel',unrelated:'private'},{'access key+雪':false},{'access key+雪':'x'.repeat(16385)}]) {
   await assert.rejects(client.openSession([plan],{queryCredentials}),e=>e.code==='InvalidConfiguration'&&!String(e).includes('secret-sentinel'));
  }
  assert.equal(attempts,0);assert.equal(plan.describe().transport.endpoint,'wss://localhost:1/events');
 }finally {globalThis.WebSocket=original;plan.dispose();c.dispose();op.dispose();doc.dispose();}
});
