import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { resolve } from 'node:path';

const root = fileURLToPath(new URL('../../..', import.meta.url));
const manifest = resolve(root, 'rust/Cargo.toml');
function run(command, args) {
  const result = spawnSync(command, args, { cwd: root, stdio: 'inherit' });
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
}
run(process.env.CARGO ?? 'cargo', ['build','--locked','--release','--target','wasm32-unknown-unknown',
  '--manifest-path',manifest,'-p','dynamic-asyncapi-wasm-bridge']);
run(process.env.WASM_BINDGEN ?? 'wasm-bindgen', ['--target','web','--out-dir',resolve(root,'packages/client/wasm'),
  '--out-name','asyncapi',resolve(process.env.CARGO_TARGET_DIR ?? resolve(root,'rust/target'),
    'wasm32-unknown-unknown/release/dynamic_asyncapi_wasm_bridge.wasm')]);
