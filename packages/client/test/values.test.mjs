import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { AsyncApiError, createClient } from '../dist/index.js';
const client = await createClient({wasm:await readFile(new URL('../wasm/asyncapi_bg.wasm',import.meta.url))});
const code = wanted => error => error instanceof AsyncApiError && error.code === wanted;

test('strict JSON owns exact numbers and rejects YAML and malformed Unicode', () => {
  const value = client.parseJson('{"id":900719925474099312345,"zero":-0}');
  const id=value.get('id'), zero=value.get('zero'); value.dispose();
  assert.equal(id.numberText,'900719925474099312345'); assert.equal(zero.numberText,'-0');
  id.dispose(); zero.dispose();
  for (const source of ['yes','a: 1','1 2','"\\ud800"','{"\\udfff":1}']) assert.throws(()=>client.parseJson(source),code('InvalidJson'));
  assert.throws(()=>client.parseJson('"\ud800"'),code('InvalidValue'));
  assert.throws(()=>client.parseJson('{"a":1,"\\u0061":2}'),code('DuplicateMember'));
});

test('plain JavaScript values retain negative zero and private-looking keys', () => {
  const input=Object.create(null); input['__proto__']='data'; input['$serde_json::private::Number']='literal'; input.zero=-0; input.items=[null,true,'😀'];
  const value=client.fromValue(input), zero=value.get('zero'), proto=value.get('__proto__');
  assert.equal(zero.numberText,'-0'); assert.equal(proto.string,'data');
  assert.deepEqual(JSON.parse(value.json),{['__proto__']:'data','$serde_json::private::Number':'literal',zero:-0,items:[null,true,'😀']});
  const literal=value.get('$serde_json::private::Number'); assert.equal(literal.string,'literal');
  literal.dispose(); zero.dispose(); proto.dispose(); value.dispose();
});

test('ordinary conversion refuses silent JSON.stringify losses and hooks', () => {
  let calls=0;
  const accessor={get secret(){calls++;return 'secret-message-123';}};
  const toJSON={toJSON(){calls++;return 1;}};
  const sparse=Array(1), extras=[]; extras.ignored=1;
  const hidden={};Object.defineProperty(hidden,'x',{value:1});
  const symbolic={[Symbol('secret')]:1}; const cycle={};cycle.self=cycle;
  for (const value of [undefined,NaN,Infinity,-Infinity,1n,()=>{},Symbol(),{x:undefined},[undefined],sparse,extras,hidden,symbolic,cycle,accessor,toJSON,new Date(),new Map(),new Number(1),'\ud800','\udfff']) {
    assert.throws(()=>client.fromValue(value),error=>code('InvalidValue')(error)&&!error.message.includes('secret-message-123'));
  }
  assert.equal(calls,0);
  const shared={x:1}; const value=client.fromValue([shared,shared]);
  assert.equal(value.json,'[{"x":1},{"x":1}]');value.dispose();
});

test('value limits reject before lossy Wasm coercions and count escaped UTF-8', () => {
  for (const limits of [{bytes:-1},{nodes:NaN},{depth:1.5},{bytes:2**32}]) assert.throws(()=>client.parseJson('null',limits),code('InvalidConfiguration'));
  for (const limits of [{bytes:4},{nodes:1},{depth:0}]) assert.throws(()=>client.fromValue(['x'],limits),code('Limit'));
  const value=client.fromValue(['x'],{bytes:5,nodes:2,depth:1});assert.equal(value.json,'["x"]');value.dispose();
  for (const [input,bytes] of [['😀',6],['\u0000',8],['\n',4],['é',4]]) {
    const exact=client.fromValue(input,{bytes});assert.equal(Buffer.byteLength(exact.json),bytes);exact.dispose();
    assert.throws(()=>client.fromValue(input,{bytes:bytes-1}),code('Limit'));
  }
  assert.throws(()=>client.parseJson('"😀"',{bytes:5}),code('Limit'));
  const empty=client.fromValue([],{depth:0});empty.dispose();
});
