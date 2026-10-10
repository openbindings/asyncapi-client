import {createServer as tlsServer} from 'node:tls';
import {createServer as httpsServer} from 'node:https';
import {createServer as tcpServer} from 'node:net';
import {execFile} from 'node:child_process';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {resolve,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {Aedes} from 'aedes';
import {WebSocketServer} from 'ws';
import {digest,profile,verifyRecords} from './peers.mjs';

const here=dirname(fileURLToPath(import.meta.url)),repo=resolve(here,'../..');
if(!process.argv[2])throw Error('Usage: node check-tls.mjs FRESH_OUTPUT_DIRECTORY');
const certs=resolve(repo,'rust/native/tests/fixtures/tls');
const out=resolve(process.argv[2]),binary=resolve(repo,'rust/target/debug/examples/exchange');
await mkdir(dirname(out),{recursive:true});await mkdir(out);
const modes=['trusted','tls12','mutual','system-roots','wrong-ca','wrong-name','expired','missing-client','deadline','missing-trust','plaintext-config'];
const cases=['mqtt','websocket'].flatMap(protocol=>modes.map(mode=>({protocol,mode}))).concat([{protocol:'mqtt',mode:'wrong-password'}]);
const report={classification:'development TLS peer observations',independent:false,startedAt:new Date().toISOString(),node:process.version,openssl:process.versions.openssl,binarySha256:digest(await readFile(binary)),sourceHashes:{},cases:[]};
const inputs=['check-tls.mjs','peers.mjs','generate-tls.py',...['fixture-ca.pem','other-ca.pem','server.pem','server.key.pem','wrong-name.pem','wrong-name.key.pem','expired.pem','expired.key.pem','client.pem','client.key.pem','identities.json'].map(name=>'tls/'+name)];
for(const name of inputs)report.sourceHashes[name]=digest(await readFile(name.startsWith('tls/')?resolve(certs,name.slice(4)):resolve(here,name)));
await writeFile(resolve(out,'plan.json'),JSON.stringify({cases,expected:'trusted/tls12/mutual/system-roots exchange two fixed 64-byte messages; trust/name/expiry/client-auth errors admit no application traffic; missing/mismatched configuration creates no TCP connection; deadline releases its socket; invalid MQTT password reaches CONNECT but no publish',claim:'Native TLS and per-session configuration only; AsyncAPI security-scheme planning, browser TLS, revocation and full qualification remain open.'},null,2));
const check=(value,message)=>{if(!value)throw Error(message);};
async function startPeer(protocol,mode) {
 const records=[],sockets=new Set();let websocket,broker;
 const certificate=['wrong-name','expired'].includes(mode)?mode:'server';
 const options={cert:await readFile(resolve(certs,certificate+'.pem')),key:await readFile(resolve(certs,certificate+'.key.pem')),ca:await readFile(resolve(certs,'fixture-ca.pem')),requestCert:['mutual','missing-client'].includes(mode),rejectUnauthorized:true,minVersion:mode==='tls12'?'TLSv1.2':'TLSv1.3',maxVersion:mode==='tls12'?'TLSv1.2':'TLSv1.3'};
 let server;
 if(mode==='deadline') server=tcpServer(socket=>socket.on('data',data=>records.push({kind:'stalled-bytes',bytes:data.length})));
 else if(protocol==='mqtt') {
  broker=await Aedes.createBroker({connectTimeout:3000});
  broker.authenticate=(_client,username,password,done)=>{const valid=username===profile.username && password?.toString()===profile.password;records.push({kind:'authenticate',valid});done(null,valid);};
  broker.on('publish',(packet,client)=>{if(client)records.push({protocol:'mqtt',topic:packet.topic,qos:packet.qos,retain:packet.retain,bytes:packet.payload.length,digest:digest(packet.payload)});});
  broker.on('clientError',()=>records.push({kind:'mqtt-client-error'}));
  server=tlsServer(options,socket=>broker.handle(socket));
 } else {
  server=httpsServer(options,(_request,response)=>{response.writeHead(404);response.end();});
  websocket=new WebSocketServer({server,path:'/events',perMessageDeflate:false,maxPayload:1024*1024});
  websocket.on('connection',socket=>{
   records.push({kind:'websocket-upgrade'});socket.send('{"kind":"unsolicited-notice"}');
   socket.on('message',(bytes,binary)=>{records.push({protocol:'websocket',binary,bytes:bytes.length,digest:digest(bytes)});socket.send(bytes,{binary});});
   socket.on('error',()=>records.push({kind:'websocket-error'}));
  });
 }
 server.on('connection',socket=>{records.push({kind:'tcp'});sockets.add(socket);socket.on('close',()=>sockets.delete(socket));socket.on('error',()=>{});});
 if(mode!=='deadline') {
  server.on('secureConnection',socket=>records.push({kind:'tls',version:socket.getProtocol(),clientAuthorized:socket.authorized,client:socket.getPeerCertificate().subject?.CN ?? null}));
  server.on('tlsClientError',error=>records.push({kind:'tls-error',code:error.code}));
 }
 await new Promise((yes,no)=>{server.once('error',no);server.listen(0,'127.0.0.1',yes);});
 return {port:server.address().port,records,sockets,async close(){for(const socket of sockets)socket.destroy();for(const socket of websocket?.clients??[])socket.terminate();await Promise.all([new Promise(yes=>server.close(yes)),...(websocket?[new Promise(yes=>websocket.close(yes))]:[]),...(broker?[new Promise(yes=>broker.close(yes))]:[])]);}};
}
for(const {protocol,mode} of cases) {
 const name=protocol+'-'+mode,peer=await startPeer(protocol,mode),item={name,protocol,mode};
 try {
  const source=JSON.parse(await readFile(resolve(here,'documents/'+(protocol==='mqtt'?'mqtt':'ws')+'-3.json'),'utf8'));
  source.servers.local.host='127.0.0.1:'+peer.port;
  source.servers.local.protocol=mode==='plaintext-config'?(protocol==='mqtt'?'mqtt':'ws'):(protocol==='mqtt'?'mqtts':'wss');
  const document=resolve(out,name+'.json');await writeFile(document,JSON.stringify(source));
  const env={...process.env};for(const key of ['ASYNCAPI_FIXTURE_CA','ASYNCAPI_FIXTURE_CERT','ASYNCAPI_FIXTURE_KEY','ASYNCAPI_FIXTURE_USERNAME','ASYNCAPI_FIXTURE_PASSWORD','ASYNCAPI_FIXTURE_CONNECT_TIMEOUT_MS','SSL_CERT_FILE','SSL_CERT_DIR'])delete env[key];
  if(mode!=='missing-trust')env.ASYNCAPI_FIXTURE_CA=resolve(certs,(mode==='wrong-ca'?'other-ca.pem':'fixture-ca.pem'));
  if(mode==='mutual'){env.ASYNCAPI_FIXTURE_CERT=resolve(certs,'client.pem');env.ASYNCAPI_FIXTURE_KEY=resolve(certs,'client.key.pem');}
  if(mode==='system-roots'){env.ASYNCAPI_FIXTURE_CA='system';env.SSL_CERT_FILE=resolve(certs,'fixture-ca.pem');env.SSL_CERT_DIR=resolve(out,'empty-trust-directory');await mkdir(env.SSL_CERT_DIR,{recursive:true});}
  if(mode==='deadline')env.ASYNCAPI_FIXTURE_CONNECT_TIMEOUT_MS='100';
  if(protocol==='mqtt'){env.ASYNCAPI_FIXTURE_USERNAME=profile.username;env.ASYNCAPI_FIXTURE_PASSWORD=mode==='wrong-password'?'intentionally-wrong':profile.password;}
  const result=await new Promise(yes=>execFile(binary,[document,'emit','listen','2','64'],{env,timeout:8000,maxBuffer:128*1024},(error,stdout,stderr)=>yes({code:error?.code??0,killed:error?.killed??false,stdout,stderr})));
  await writeFile(resolve(out,name+'.stdout'),result.stdout);await writeFile(resolve(out,name+'.stderr'),result.stderr);
  item.exitCode=result.code;check(!result.killed,'consumer exceeded its fixture deadline');
  const success=['trusted','tls12','mutual','system-roots'].includes(mode);
  if(success) {
   check(result.code===0,'expected successful TLS consumer: '+result.stderr);
   item.peer=verifyRecords(peer.records,{protocol,count:2,size:64});
   const handshake=peer.records.find(r=>r.kind==='tls');check(handshake?.version===(mode==='tls12'?'TLSv1.2':'TLSv1.3'),'wrong TLS protocol');
   if(mode==='mutual')check(handshake.clientAuthorized && handshake.client==='fixture-client','client identity was not verified');
  } else {
   check(result.code!==0,'expected refusal');check(!peer.records.some(r=>r.digest),'failed TLS/authentication leaked application messages');
   const expected=['missing-trust','plaintext-config'].includes(mode)?'InvalidConfiguration':mode==='deadline'?'Deadline':'Connection';
   check(result.stderr.includes('code: '+expected),'unexpected runtime refusal: '+result.stderr);
   if(['missing-trust','plaintext-config'].includes(mode))check(!peer.records.some(r=>r.kind==='tcp'),'invalid configuration opened a socket');
   if(['wrong-ca','wrong-name','expired','missing-client'].includes(mode))check(!peer.records.some(r=>r.kind==='authenticate'||r.kind==='websocket-upgrade'),'TLS rejection reached the application protocol');
   if(mode==='wrong-password')check(peer.records.some(r=>r.kind==='authenticate' && !r.valid),'MQTT rejection was not an authentication attempt');
  }
  await new Promise(yes=>setTimeout(yes,30));check(peer.sockets.size===0,'consumer left a TCP socket open');
  item.status='passed';
 } catch(error){item.status='failed';item.error=String(error.stack??error);}
 finally {item.records=peer.records;await peer.close();report.cases.push(item);await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2));}
}
report.status=report.cases.every(c=>c.status==='passed')?'passed':'failed';
await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2)+'\n');
console.log(JSON.stringify({out,status:report.status,passed:report.cases.filter(c=>c.status==='passed').length,total:cases.length,failures:report.cases.filter(c=>c.status==='failed').map(c=>({name:c.name,error:c.error}))}));
if(report.status!=='passed')process.exitCode=1;
