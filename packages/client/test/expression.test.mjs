import assert from 'node:assert/strict';
import {readFile} from 'node:fs/promises';
import test from 'node:test';
import {createClient,AsyncApiError} from '../dist/index.js';
const client=await createClient({wasm:await readFile(new URL('../wasm/asyncapi_bg.wasm',import.meta.url))});

test('runtime expressions preserve exact owning values through the Rust facade',()=>{
 const expression=client.runtimeExpression('$message.payload#/id'),payload=client.parseJson('{"id":9007199254740993123456789}'),header=client.fromValue({id:'different'});
 const result=expression.evaluate({header,payload});
 assert.deepEqual(expression.describe(),{expression:'$message.payload#/id',source:'payload',pointer:'/id'});
 const detached=expression.describe();detached.source='header';
 assert.equal(expression.describe().source,'payload');
 assert.equal(expression.evaluate({header}),undefined);
 expression.dispose();payload.dispose();header.dispose();
 try {assert.equal(result.kind,'number');assert.equal(result.numberText,'9007199254740993123456789');}
 finally {result.dispose();}
 assert.throws(()=>expression.evaluate({}),/disposed/i);
});

test('runtime expressions distinguish absent paths and JSON null without URI decoding',()=>{
 const payload=client.fromValue({'a%2Fb':null,a:{b:'nested'},'a/b':{'~key':['雪']}});
 for(const [path,expected] of [['/a%2Fb','null'],['/a~1b/~0key/0','"雪"'],['/missing',undefined]]) {
  const expression=client.runtimeExpression('$message.payload#'+path),result=expression.evaluate({payload});
  try {assert.equal(result?.json,expected);}finally {result?.dispose();expression.dispose();}
 }
 payload.dispose();
 for(const source of ['$message.payload#not-a-pointer','$message.headers#/id','$message.payload#/x~2'])assert.throws(()=>client.runtimeExpression(source),e=>e instanceof AsyncApiError&&e.code==='InvalidValue');
 assert.throws(()=>client.runtimeExpression('$message.payload#'+'/x'.repeat(257)),e=>e.code==='Limit');
 for(const input of [undefined,1,'$message.payload#/\ud800'])assert.throws(()=>client.runtimeExpression(input),e=>e.code==='InvalidValue');
 assert.throws(()=>client.runtimeExpression('$message.payload#/'+'雪'.repeat(5461)),e=>e.code==='Limit');
});

test('correlation metadata resolves effective declarations without enabling execution',()=>{
 const source={asyncapi:'3.1.0',info:{title:'correlation',version:'1'},servers:{s:{protocol:'ws',host:'localhost:8080'}},channels:{c:{address:'/events',messages:{m:{contentType:'application/json',traits:[{correlationId:{location:'$message.payload#/id',description:'inherited'}}],correlationId:{description:'local'}}}}},operations:{emit:{action:'send',channel:{$ref:'#/channels/c'}}}};
 const doc=client.parse(JSON.stringify(source)),op=doc.operation('emit'),compiled=op.compile();doc.dispose();op.dispose();
 try {
  const correlation=compiled.correlation('m');assert.equal(correlation.description,'local');assert.equal(correlation.location.pointer,'/channels/c/messages/m/traits/0/correlationId/location');assert.equal(compiled.describe().messages[0].correlationId.pointer,'/channels/c/messages/m/correlationId');
  const expression=client.runtimeExpression(correlation.expression.expression),payload=client.fromValue({id:'reply-1'}),id=expression.evaluate({payload});
  try{assert.equal(id.string,'reply-1');}finally{id.dispose();payload.dispose();expression.dispose();}
  assert.throws(()=>compiled.correlation('missing'),e=>e.code==='InvalidConfiguration');
  assert.throws(()=>compiled.prepare({role:'application'}),e=>e.code==='UnsupportedFeature');
 }finally{compiled.dispose();}
});
