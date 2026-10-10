import {createServer} from 'node:http';
import {spawn,spawnSync} from 'node:child_process';
import {createRequire} from 'node:module';
import {createHash} from 'node:crypto';
import {readFile} from 'node:fs/promises';
import {mkdir,writeFile} from 'node:fs/promises';
import {dirname,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
const here=dirname(fileURLToPath(import.meta.url));const repo=resolve(here,'../..');
const require=createRequire(resolve(process.env.ASYNCAPI_HOST_TOOLS??here,'package.json'));
const binary=process.env.ASYNCAPI_WORKERD??require('workerd').default;
const {startPeers}=await import(resolve(repo,'qualification/fixtures/peers.mjs'));
const mode=process.argv[2];if(!['plain','wasm-grow'].includes(mode)||!process.argv[3])throw new Error('Usage: probe-worker-close.mjs plain|wasm-grow FRESH_OUTPUT_DIRECTORY');
const out=resolve(process.argv[3]);await mkdir(dirname(out),{recursive:true});await mkdir(out);
const peers=await startPeers();const reserve=createServer();await new Promise(r=>reserve.listen(0,'127.0.0.1',r));const port=reserve.address().port;await new Promise(r=>reserve.close(r));
const entry=`export default { async fetch(request){if(new URL(request.url).pathname==='/ready')return new Response('ready');
const results=[];
for(let cycle=0;cycle<100;cycle++) results.push(await new Promise(resolve=>{
 const socket=new WebSocket('ws://127.0.0.1:${peers.websocketPort}/events');socket.binaryType='arraybuffer';
 let done=false;const events=[];const finish=()=>{if(done)return;done=true;clearTimeout(timer);socket.close();resolve({cycle,events});};
 const timer=setTimeout(()=>{events.push({type:'deadline'});finish();},2000);
 socket.addEventListener('open',()=>{events.push({type:'open'});${mode==='plain'?'socket.send(new Uint8Array(1024));':'const memory=new WebAssembly.Memory({initial:1});const bytes=new Uint8Array(memory.buffer,0,1024);socket.send(bytes);memory.grow(1);'}});
 socket.addEventListener('message',event=>{if(typeof event.data==='string')return;events.push({type:'binary',bytes:event.data.byteLength});socket.close(1000);});
 socket.addEventListener('error',event=>{events.push({type:'error',message:event.message,readyState:socket.readyState});finish();});
 socket.addEventListener('close',event=>{events.push({type:'close',code:event.code,wasClean:event.wasClean});finish();});
}));
return Response.json(results); }};`;
await writeFile(resolve(out,'worker.mjs'),entry);
await writeFile(resolve(out,'config.capnp'),`using Workerd = import "/workerd/workerd.capnp";
const config :Workerd.Config = (services=[(name="main",worker=(compatibilityDate="2026-10-08",modules=[(name="worker.mjs",esModule=embed "worker.mjs")])),(name="internet",network=(allow=["private"]))],sockets=[(name="http",address="127.0.0.1:${port}",http=(),service="main")]);`);
const child=spawn(binary,['serve','config.capnp'],{cwd:out,stdio:['ignore','pipe','pipe']});let logs='';child.stdout.on('data',x=>logs+=x);child.stderr.on('data',x=>logs+=x);
const report={classification:'direct host control, no Rust or client facade; a reproduction is a failure observation, not proof of cause',mode,workerd:spawnSync(binary,['--version'],{encoding:'utf8'}).stdout.trim(),compatibilityDate:'2026-10-08',harnessSha256:createHash('sha256').update(await readFile(fileURLToPath(import.meta.url))).digest('hex')};
try {let response;for(let i=0;i<60;i++){try{response=await fetch(`http://127.0.0.1:${port}/ready`,{signal:AbortSignal.timeout(1000)});break;}catch{await new Promise(r=>setTimeout(r,100));}}
if(!response?.ok)throw new Error(`status ${response?.status}`);response=await fetch(`http://127.0.0.1:${port}/`,{signal:AbortSignal.timeout(30000)});if(!response.ok)throw new Error(`probe status ${response.status}`);report.results=await response.json();report.failures=report.results.filter(r=>r.events.some(e=>e.type==='error'||e.type==='deadline'));report.status=report.failures.length?'reproduced':'not reproduced';
} catch(error){report.error=String(error);}finally{child.kill('SIGTERM');await new Promise(r=>child.once('exit',r));await writeFile(resolve(out,'workerd.log'),logs);await writeFile(resolve(out,'peer.json'),JSON.stringify(peers.records,null,2));await peers.close();await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2));console.log(JSON.stringify({out,status:report.status,failures:report.failures,error:report.error}));}
