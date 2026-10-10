import { createServer } from 'node:http';
import { spawn, spawnSync } from 'node:child_process';
import { createRequire } from 'node:module';
import { mkdir, readFile, writeFile, copyFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { resolve, dirname } from 'node:path';
import { createHash } from 'node:crypto';

const here = dirname(fileURLToPath(import.meta.url));
const repo = resolve(here, '../..');
const {startPeers,verifyRecords}=await import(resolve(repo,'qualification/fixtures/peers.mjs'));
const {startHostFaults}=await import(resolve(repo,'qualification/fixtures/host-faults.mjs'));
const hostTools = process.env.ASYNCAPI_HOST_TOOLS ?? here;
const compositionDir = process.env.ASYNCAPI_COMPOSITION_WASM ?? resolve(repo,'qualification/composition/wasm');
const suite = process.argv[2];
if(!['exchange','faults'].includes(suite) || !process.argv[3]) throw new Error('Usage: node run.mjs exchange|faults FRESH_OUTPUT_DIRECTORY');
const faultSuite = suite==='faults';
const makePeers = faultSuite ? startHostFaults : startPeers;
const require = createRequire(resolve(hostTools,'package.json'));
const { chromium } = require('playwright-core');
const esbuild = require('esbuild');
const workerd = process.env.ASYNCAPI_WORKERD ?? require('workerd').default;
const out = resolve(process.argv[3]);
await mkdir(dirname(out),{recursive:true});
await mkdir(out);
const report = { classification:'development', independent:false, suite, platform:process.platform, architecture:process.arch, startedAt:new Date().toISOString(), esbuild:esbuild.version,
  workerd:spawnSync(workerd,['--version'],{encoding:'utf8'}).stdout.trim(), compatibilityDate:'2026-10-08', checks:{} };
const sources = {};
for (const [name, path] of Object.entries({
  'facade.js':resolve(repo,'packages/client/dist/index.js'),
  'json-input.js':resolve(repo,'packages/client/dist/json-input.js'),
  'asyncapi.js':resolve(repo,'packages/client/wasm/asyncapi.js'),
  'asyncapi.wasm':resolve(repo,'packages/client/wasm/asyncapi_bg.wasm'),
  'composition.js':resolve(compositionDir,'composition.js'),
  'composition.wasm':resolve(compositionDir,'composition_bg.wasm'),
  'consumer.mjs':resolve(here,faultSuite?'fault-consumer.mjs':'consumer.mjs'),
})) {
  const bytes = await readFile(path);
  sources[name] = bytes;
  report[name] = { sha256:createHash('sha256').update(bytes).digest('hex'),bytes:bytes.length };
}
report.sourceHashes={};
for(const path of [fileURLToPath(import.meta.url),resolve(repo,'qualification/fixtures/peers.mjs'),resolve(repo,'qualification/fixtures/host-faults.mjs')]) report.sourceHashes[path]=createHash('sha256').update(await readFile(path)).digest('hex');
let browser, child, peers;
const routes = {
  '/': { type:'text/html', bytes:Buffer.from('<!doctype html><title>AsyncAPI inspection development consumer</title>') },
  '/client.js': { type:'text/javascript',bytes:sources['facade.js'] },
  '/json-input.js': { type:'text/javascript',bytes:sources['json-input.js'] },
  '/wasm/asyncapi.js':{ type:'text/javascript',bytes:sources['asyncapi.js'] },
  '/wasm/asyncapi_bg.wasm':{ type:'application/wasm',bytes:sources['asyncapi.wasm'] },
  '/composition.js':{ type:'text/javascript',bytes:sources['composition.js'] },
  '/composition_bg.wasm':{ type:'application/wasm',bytes:sources['composition.wasm'] },
  '/consumer.mjs':{ type:'text/javascript',bytes:sources['consumer.mjs'] },
};
const server = createServer(async (req,res) => {
  if (/^\/(wasm\/)?snippets\/[A-Za-z0-9_./-]+$/.test(req.url) && !req.url.includes('..')) {
    const base=req.url.startsWith('/wasm/')?resolve(repo,'packages/client/wasm'):compositionDir;
    const relative=req.url.replace(/^\/(wasm\/)?/,'');
    try {const bytes=await readFile(resolve(base,relative));res.writeHead(200,{'content-type':'text/javascript'});res.end(bytes);return;}catch {res.writeHead(404);res.end();return;}
  }
  const route = routes[req.url];
  if (!route) { res.writeHead(404);res.end();return; }
  res.writeHead(200,{'content-type':route.type});res.end(route.bytes);
});
try {
  await new Promise((resolve,reject) => { server.once('error',reject);server.listen(0,'127.0.0.1',resolve); });
  browser = await chromium.launch({executablePath:process.env.ASYNCAPI_CHROME ?? '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',headless:true});
  report.browser = browser.version();
  const page = await browser.newPage();
  await page.goto(`http://127.0.0.1:${server.address().port}/`);
  peers=await makePeers();
  const template=await readFile(resolve(repo,'qualification/fixtures/documents/ws-3.json'),'utf8');
  let source=template.replaceAll('__PORT__',String(peers.websocketPort ?? peers.port));
  report.checks.browser = await page.evaluate(async ({source,faultSuite}) => {
    const {createClient} = await import('/client.js');
    const composition = await import('/composition.js');
    const consumer = await import('/consumer.mjs');
    const [client] = await Promise.all([createClient(), composition.default()]);
    return faultSuite ? consumer.exerciseFaults(client,source,Number(JSON.parse(source).servers.local.host.split(':')[1])) : consumer.exerciseSessions(client, composition.exchange_from_outer_api, source);
  },{source,faultSuite});
  const browserRecords=peers.records.filter(r=>r.digest);
  if(!faultSuite) report.browserPeer=[verifyRecords(browserRecords.slice(0,8),{protocol:'websocket',count:8,size:1024}),verifyRecords(browserRecords.slice(8),{protocol:'websocket',count:8,size:1024})];
  await writeFile(resolve(out,'browser-peer.json'),JSON.stringify(peers.records,null,2));
  await peers.close();peers=await makePeers();source=template.replaceAll('__PORT__',String(peers.websocketPort ?? peers.port));
  await browser.close(); browser = undefined;
  await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2));
  const workerEntry = `import {createClient} from ${JSON.stringify(resolve(repo,'packages/client/dist/index.js'))};
import compositionInit, {exchange_from_outer_api} from ${JSON.stringify(resolve(compositionDir,'composition.js'))};
import * as consumer from ${JSON.stringify(resolve(here,faultSuite?'fault-consumer.mjs':'consumer.mjs'))};
import engineModule from './engine.wasm';
import compositionModule from './composition.wasm';
export default { async fetch(request) {
  if(new URL(request.url).pathname==='/ready') return new Response('ready');
  const [client] = await Promise.all([createClient({wasm:engineModule}),compositionInit({module_or_path:compositionModule})]);
  const raw=${faultSuite?'[]':`await consumer.rawCloseControl(${JSON.stringify(source)})`};
  try { return Response.json({raw,...await ${faultSuite?'consumer.exerciseFaults(client, '+JSON.stringify(source)+', '+peers.port+')':'consumer.exerciseSessions(client,exchange_from_outer_api,'+JSON.stringify(source)+')'}}); } catch(error) { return Response.json({raw,error:String(error)},{status:500}); }
}};`;
  await writeFile(resolve(out,'worker-entry.mjs'),workerEntry);
  await esbuild.build({stdin:{contents:workerEntry,resolveDir:out,sourcefile:'worker-entry.mjs'},bundle:true,format:'esm',platform:'browser',target:'es2022',external:['./engine.wasm','./composition.wasm'],outfile:resolve(out,'worker.mjs')});
  await writeFile(resolve(out,'engine.wasm'),sources['asyncapi.wasm']);
  await writeFile(resolve(out,'composition.wasm'),sources['composition.wasm']);
  // Ask the OS for a spare loopback port; close the reservation before workerd binds.
  const reservation = createServer();
  await new Promise(resolve => reservation.listen(0,'127.0.0.1',resolve));
  const port = reservation.address().port;
  await new Promise(resolve => reservation.close(resolve));
  await writeFile(resolve(out,'config.capnp'),`using Workerd = import "/workerd/workerd.capnp";
const config :Workerd.Config = (
 services = [(name="main",worker=(compatibilityDate="2026-10-08",modules=[
  (name="worker.mjs",esModule=embed "worker.mjs"),
  (name="engine.wasm",wasm=embed "engine.wasm"),
  (name="composition.wasm",wasm=embed "composition.wasm")
 ])),(name="internet",network=(allow=["private"]))], sockets=[(name="http",address="127.0.0.1:${port}",http=(),service="main")]);`);
  child = spawn(workerd,['serve','config.capnp'],{cwd:out,stdio:['ignore','pipe','pipe']});
  let logs = '';
  child.stdout.on('data',bytes => { logs += bytes; });
  child.stderr.on('data',bytes => { logs += bytes; });
  let response;
  for(let attempt=0;attempt<60;attempt++) {
    if(child.exitCode !== null) throw new Error(`workerd exited ${child.exitCode}: ${logs}`);
    try { response = await fetch(`http://127.0.0.1:${port}/ready`,{signal:AbortSignal.timeout(1000)});break; }
    catch { await new Promise(resolve => setTimeout(resolve,100)); }
  }
  if(!response?.ok) throw new Error('workerd did not become ready');
  response=await fetch(`http://127.0.0.1:${port}/`,{signal:AbortSignal.timeout(15000)});
  await writeFile(resolve(out,'workerd.log'),logs);
  if(!response?.ok) throw new Error(`workerd response ${response?.status}: ${await response?.text()}`);
  report.checks.workerd = await response.json();
  const workerRecords=peers.records.filter(r=>r.digest);
  if(!faultSuite) report.workerPeer=[verifyRecords(workerRecords.slice(0,8),{protocol:'websocket',count:8,size:1024}),verifyRecords(workerRecords.slice(8),{protocol:'websocket',count:8,size:1024})];
  await writeFile(resolve(out,'workerd-peer.json'),JSON.stringify(peers.records,null,2));
  report.status = 'passed';
} catch(error) {
  report.status = 'failed';report.error = String(error.stack ?? error);process.exitCode = 1;
} finally {
  if(peers) {await writeFile(resolve(out,'last-peer.json'),JSON.stringify(peers.records,null,2));await peers.close();}
  if(browser) await browser.close();
  if(child && child.exitCode === null) { child.kill('SIGTERM');await new Promise(resolve => child.once('exit',resolve)); }
  await new Promise(resolve => server.close(resolve));
  await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2)+'\n');
  console.log(JSON.stringify({out,status:report.status,browser:report.browser,workerd:report.workerd,error:report.error?.slice(0,1000)}));
}
