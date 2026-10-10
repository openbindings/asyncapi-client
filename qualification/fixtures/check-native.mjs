import {startPeers,profile,verifyRecords} from './peers.mjs';
import {spawn} from 'node:child_process';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import {fileURLToPath} from 'node:url';
import {dirname,resolve} from 'node:path';
const here=dirname(fileURLToPath(import.meta.url));
const binary=process.env.ASYNCAPI_EXCHANGE_BINARY??resolve(here,'../../rust/target/debug/examples/exchange');
const out=process.argv[2];
if(!out) throw new Error('Provide an output directory for retained development evidence');
await mkdir(dirname(resolve(out)),{recursive:true});
await mkdir(out);
const report={classification:'dynamic-client development execution with independent peer observations; no independent authorship claim',independent:false,binarySha256:createHash('sha256').update(await readFile(binary)).digest('hex'),cases:[],sourceHashes:{}};
for(const file of ['peers.mjs','check-native.mjs','../../rust/Cargo.lock','../../rust/native/src/lib.rs','../../rust/session/src/plan.rs','../../rust/session/src/budget.rs','../../rust/session/src/error.rs','../../rust/native/src/mqtt.rs','../../rust/native/src/websocket.rs','../../rust/native/examples/exchange.rs']) {report.sourceHashes[file]=createHash('sha256').update(await readFile(resolve(here,file))).digest('hex');}
async function run(protocol,edition,format,size,wrongRoute=false,qos=1) {
 const peers=await startPeers();
 const name=`${protocol}-${edition}-${format}-${size}${wrongRoute?'-wrong-route':''}${protocol==='mqtt'?'-qos'+qos:''}`;
 const item={name,protocol,edition,format,size,wrongRoute,qos};
 try {
  const template=await readFile(resolve(here,`documents/${protocol}-${edition.startsWith('2')?'2':'3'}.${format}`),'utf8');
  let source=template.replaceAll('__PORT__',String(protocol==='mqtt'?peers.mqttPort:peers.websocketPort)).replaceAll('3.1.0',edition);
  if(protocol==='mqtt') source=source.replaceAll(/(qos["']?\s*:\s*)1/g,(_match,prefix)=>prefix+qos);
  if(wrongRoute) source=source.replaceAll('fixture/events','wrong/events');
  const path=resolve(out,`${name}.${format}`);await writeFile(path,source);item.sourceSha256=createHash('sha256').update(source).digest('hex');
  const count=8;
  const env={...process.env};delete env.ASYNCAPI_FIXTURE_USERNAME;delete env.ASYNCAPI_FIXTURE_PASSWORD;
  if(protocol==='mqtt') { env.ASYNCAPI_FIXTURE_USERNAME=profile.username;env.ASYNCAPI_FIXTURE_PASSWORD=profile.password; }
  const child=spawn(binary,[path,'emit','listen',String(count),String(size)],{env,stdio:['ignore','pipe','pipe']});
  let stdout='',stderr='';child.stdout.on('data',x=>stdout+=x);child.stderr.on('data',x=>stderr+=x);
  const timer=setTimeout(()=>child.kill('SIGKILL'),15000);
  item.exit=await new Promise((resolve,reject)=>{child.once('error',reject);child.once('exit',(code,signal)=>resolve({code,signal}));});clearTimeout(timer);
  await writeFile(resolve(out,name+'.stdout'),stdout);await writeFile(resolve(out,name+'.stderr'),stderr);
  await writeFile(resolve(out,name+'.peer.json'),JSON.stringify(peers.records,null,2));
  if(item.exit.code!==0) throw new Error('native consumer failed: '+stderr);
  item.consumer=JSON.parse(stdout);
  if(item.consumer.received!==count || item.consumer.rejected!==(protocol==='ws'?1:0) || item.consumer.afterClose!=='Closed') throw new Error('consumer observations differ');
  if(item.consumer.receipts.length!==count || item.consumer.receipts.some(r=>r.kind!==(protocol==='mqtt'?(qos===0?'mqttPublishFlushed':'mqttPubAck'):'webSocketFlushed'))) throw new Error('receipt meaning differs');
  if(protocol==='mqtt' && (item.consumer.subscriptions.length!==1 || item.consumer.subscriptions[0].operation!==1 || item.consumer.subscriptions[0].requestedQos!==qos || item.consumer.subscriptions[0].grantedQos!==qos || item.consumer.receivedQos.length!==count || item.consumer.receivedQos.some(actual=>actual!==qos))) throw new Error('negotiated or received QoS differs');
  if(peers.truncated) throw new Error('peer trace truncated');
  const connect=peers.records.find(r=>r.kind==='connect' && r.protocol===(protocol==='mqtt'?'mqtt':'websocket'));
  if(protocol==='mqtt' && (!connect || connect.client!=='asyncapi-fixture' || connect.version!==4 || connect.clean!==true || connect.keepalive!==1)) throw new Error('MQTT CONNECT settings differ');
  if(protocol==='ws' && (!connect || connect.path!=='/events' || connect.method!=='GET')) throw new Error('WebSocket handshake settings differ');
  if(wrongRoute) { let rejected=false;try{verifyRecords(peers.records,{protocol:'mqtt',count,size});}catch(error){rejected=true;item.expectedRejection=String(error);}if(!rejected)throw new Error('wrong route was not detected'); }
  else item.peer=verifyRecords(peers.records,{protocol:protocol==='mqtt'?'mqtt':'websocket',count,size,qos});
  item.status='passed';
 } catch(error) {item.status='failed';item.error=String(error.stack??error);}
 finally {await peers.close();report.cases.push(item);await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2)+'\n');console.log(JSON.stringify({name,status:item.status,error:item.error}));}
}
for(const protocol of ['mqtt','ws']) for(const edition of ['2.6.0','3.0.0','3.1.0']) for(const format of ['json','yaml']) await run(protocol,edition,format,1024);
for(const protocol of ['mqtt','ws']) await run(protocol,'3.1.0','yaml',65536);
await run('mqtt','3.1.0','json',64,true);
for(const edition of ['2.6.0','3.0.0','3.1.0']) for(const format of ['json','yaml']) await run('mqtt',edition,format,1024,false,0);
await run('mqtt','3.1.0','yaml',65536,false,0);
report.status=report.cases.every(c=>c.status==='passed')?'passed':'failed';await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2)+'\n');
console.log(JSON.stringify({out,status:report.status}));if(report.status==='failed')process.exitCode=1;
