import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { AsyncApiError, createClient } from '../dist/index.js';

const client = await createClient({ wasm: await readFile(new URL('../wasm/asyncapi_bg.wasm', import.meta.url)) });
const source = '{"asyncapi":"3.1.0","info":{"title":"test","version":"1"},"channels":{"events":{"address":null}},"operations":{"emit":{"action":"send","channel":{"$ref":"#/channels/events"}}},"x-data":{"huge":900719925474099312345,"zero":-0,"nil":null}}';

test('YAML values retain exact JSON and explicit alias provenance', () => {
  const source = "asyncapi: 2.6.0\ninfo: {title: YAML, version: '1'}\nchannels:\n  events:\n    subscribe: {operationId: emit}\nx-base: &base {n: 0xffffffffffffffffffff}\nx-copy: *base\n";
  const document = client.parse(source);
  const operation = document.operation('emit');
  const root = document.root;
  const value = root.pointer('/x-copy/n');
  assert.equal(operation.describe().action, 'send');
  assert.equal(value.numberText, '1208925819614629174706175');
  assert.equal(value.raw, '0xffffffffffffffffffff');
  assert.equal(value.json, '1208925819614629174706175');
  assert.equal(value.location.aliases.length, 1);
  assert.equal(value.location.pointer, '/x-copy/n');
  operation.dispose(); root.dispose(); document.dispose();
  assert.equal(value.json, '1208925819614629174706175');
  value.dispose();
});

test('public facade uses native identity, authored locations, and application direction', () => {
  const document = client.parse(source, { sourceUri:'https://example.test/api.json' });
  const op = document.operation('emit');
  assert.equal(document.version, '3.1.0');
  assert.deepEqual(op.identity, { uri:'https://example.test/api.json', pointer:'/operations/emit' });
  assert.equal(op.describe().action, 'send');
  assert.equal(op.describe().address, null);
  assert.equal(op.location.pointer, '/operations/emit');
  document.dispose();
  assert.equal(op.describe().operationId, 'emit');
  op.dispose();
  op.dispose();
  assert.throws(() => op.describe(), error => error instanceof AsyncApiError && error.code === 'Disposed');
});

test('exact tokens and absent/null survive the Wasm boundary', () => {
  const document = client.parse(source);
  const root = document.root;
  const huge = root.pointer('/x-data/huge');
  const zero = root.pointer('/x-data/zero');
  const nil = root.pointer('/x-data/nil');
  assert.equal(huge.numberText, '900719925474099312345');
  assert.equal(zero.numberText, '-0');
  assert.equal(nil.kind, 'null');
  assert.equal(root.pointer('/x-data/absent'), undefined);
  assert.equal(root.at(-1), undefined);
  assert.equal(root.at(2 ** 32), undefined);
  document.dispose(); root.dispose();
  assert.equal(huge.raw, '900719925474099312345');
  huge.dispose(); zero.dispose(); nil.dispose();
});

test('missing source is structured, and completion keeps the old snapshot', () => {
  const source = '{"asyncapi":"2.6.0","info":{"title":"test","version":"1"},"channels":{"events":{"$ref":"channel.json"}}}';
  const original = client.parse(source, { sourceUri:'https://example.test/api.json' });
  assert.throws(() => original.operationAt('/channels/events/subscribe'), error => {
    assert.equal(error.code, 'MissingResource');
    assert.deepEqual(error.requirement, { kind:'resource', uri:'https://example.test/channel.json' });
    return true;
  });
  const complete = original.withResource('https://example.test/channel.json', '{"subscribe":{"operationId":"emit"}}');
  const op = complete.operation('emit');
  complete.dispose();
  assert.equal(op.describe().action, 'send');
  assert.equal(op.identity.pointer, '/channels/events/subscribe');
  assert.equal(op.location.pointer, '/subscribe');
  assert.throws(() => original.operation('emit'), error => error.code === 'MissingResource');
  op.dispose(); original.dispose();
});

test('partial inventory preserves failures and releases its cursor on early exit', () => {
  const doc = client.parse('{"asyncapi":"2.6.0","info":{"title":"test","version":"1"},"channels":{"bad":false,"good":{"subscribe":{}}}}');
  const entries = [...doc.operations()];
  assert.equal(entries.length, 2);
  assert.equal(entries[0].ok, false);
  assert.equal(entries[0].error.code, 'InvalidDocument');
  assert.equal(entries[1].ok, true);
  entries[1].value.dispose();
  const cursor = doc.operations();
  assert.equal(cursor.next().value.ok, false);
  cursor.return();
  assert.equal(cursor.next().done, true);
  doc[Symbol.dispose]();
});

test('valid serde-looking application property is preserved; duplicates remain errors', () => {
  const doc = client.parse(source.replace('"x-data":{', '"x-data":{"$serde_json::private::Number":"42",'));
  const root = doc.root;
  const value = root.pointer('/x-data/$serde_json::private::Number');
  assert.equal(value.string, '42');
  value.dispose(); root.dispose(); doc.dispose();
  assert.throws(() => client.parse(source.replace('"address":null', '"address":null,"address":"second"')), error => error.code === 'DuplicateMember');
});

const binarySource = JSON.stringify({
  asyncapi:'3.1.0', info:{title:'binary',version:'1'},
  servers:{local:{host:'example.test',protocol:'wss'}},
  channels:{events:{address:'/events',messages:{event:{contentType:'application/octet-stream'}}}},
  operations:{emit:{action:'send',channel:{$ref:'#/channels/events'}}},
});
test('Rust compilation and pure preparation survive disposal and preserve typed choices', () => {
  const doc = client.parse(binarySource);
  const operation = doc.operation('emit');
  const compiled = operation.compile();
  operation.dispose(); doc.dispose();
  const message = compiled.messageSource('event');
  const contentType=message.get('contentType');
  assert.equal(contentType.string, 'application/octet-stream');
  contentType.dispose();
  message.dispose();
  assert.equal(compiled.describe().messages[0].selection.pointer, '/channels/events/messages/event');
  const plan = compiled.prepare({role:'application'});
  assert.deepEqual(plan.describe().transport, {kind:'webSocket6455',endpoint:'wss://example.test/events',method:'GET',frame:'binary'});
  assert.throws(() => compiled.prepare({role:'peer'}), error => error instanceof AsyncApiError && error.requirement.kind === 'peerRoute');
  assert.throws(() => compiled.prepare({role:'application',surprise:true}), error => error.code === 'InvalidConfiguration');
  compiled.dispose();
  assert.equal(plan.describe().wireAction, 'send');
  plan.dispose();
  assert.throws(() => plan.describe(), error => error.code === 'Disposed');
});

test('empty messages and codec requirements survive the TypeScript boundary distinctly', () => {
  for (const [changed, code, requirement] of [
    [binarySource.replace('"contentType":"application/octet-stream"', ''), 'UnsupportedFeature', {kind:'codec',contentType:null}],
    [binarySource.replace('"action":"send"', '"action":"send","messages":[]'), 'NoMessages', null],
  ]) {
    const doc=client.parse(changed);const operation=doc.operation('emit');const compiled=operation.compile();
    assert.throws(() => compiled.prepare({role:'application'}), error => {
      assert.equal(error.code,code);assert.deepEqual(error.requirement,requirement);return true;
    });
    compiled.dispose();operation.dispose();doc.dispose();
  }
});

test('JSON and text codecs expose frame policy while schema obligations remain explicit', () => {
  for(const [contentType,codec] of [['application/json','json'],['text/plain; charset=utf-8','utf8']]) {
    const document=client.parse(binarySource.replace('application/octet-stream',contentType));
    const operation=document.operation('emit'), compiled=operation.compile();
    const automatic=compiled.prepare({role:'application'}), binary=compiled.prepare({role:'application',websocketFrame:'binary'});
    assert.equal(automatic.describe().codec,codec);assert.equal(automatic.describe().transport.frame,'text');assert.equal(binary.describe().transport.frame,'binary');
    automatic.dispose();binary.dispose();compiled.dispose();operation.dispose();document.dispose();
  }
  const source=JSON.parse(binarySource);source.channels.events.messages.event.contentType='application/json';source.channels.events.messages.event.payload={type:'object'};
  const document=client.parse(JSON.stringify(source)), operation=document.operation('emit'),compiled=operation.compile();
  assert.throws(()=>compiled.prepare({role:'application'}),error=>error.requirement?.kind==='evaluator');
  compiled.dispose();operation.dispose();document.dispose();
});
