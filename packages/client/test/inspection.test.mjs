import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import test from 'node:test';
import { AsyncApiError, createClient } from '../dist/index.js';

const client = await createClient({ wasm: await readFile(new URL('../wasm/asyncapi_bg.wasm', import.meta.url)) });
const source = '{"asyncapi":"3.1.0","info":{"title":"test","version":"1"},"channels":{"events":{"address":null}},"operations":{"emit":{"action":"send","channel":{"$ref":"#/channels/events"}}},"x-data":{"huge":900719925474099312345,"zero":-0,"nil":null}}';

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
