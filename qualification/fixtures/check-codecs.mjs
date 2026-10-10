import {startPeers,profile,digest} from './peers.mjs';
import {spawn} from 'node:child_process';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {fileURLToPath} from 'node:url';
import {dirname,resolve} from 'node:path';
const here=dirname(fileURLToPath(import.meta.url)),out=resolve(process.argv[2]);
await mkdir(out,{recursive:false});
const binary=resolve(here,'../../rust/target/debug/examples/codecs');
const report={classification:'development codec execution with distinct wire peer observations',independent:false,binarySha256:digest(await readFile(binary)),cases:[],sourceHashes:{}};
for(const name of ['check-codecs.mjs','peers.mjs','../../rust/native/examples/codecs.rs']) report.sourceHashes[name]=digest(await readFile(resolve(here,name)));
async function run(protocol,edition,format,codec,frame='default') {
 const name=`${protocol}-${edition}-${format}-${codec}-${frame}`,peers=await startPeers();let item={name,protocol,edition,format,codec,frame};
 try {
  let source=(await readFile(resolve(here,`documents/${protocol==='mqtt'?'mqtt':'ws'}-${edition.startsWith('2')?'2':'3'}.${format}`),'utf8')).replaceAll('__PORT__',String(protocol==='mqtt'?peers.mqttPort:peers.websocketPort)).replaceAll('3.1.0',edition).replaceAll('application/octet-stream',codec==='json'?'application/json':'text/plain; charset=utf-8');
  const path=resolve(out,name+'.'+format);await writeFile(path,source);item.sourceSha256=digest(Buffer.from(source));
  const env={...process.env};delete env.ASYNCAPI_FIXTURE_USERNAME;delete env.ASYNCAPI_FIXTURE_PASSWORD;
  if(protocol==='mqtt') {env.ASYNCAPI_FIXTURE_USERNAME=profile.username;env.ASYNCAPI_FIXTURE_PASSWORD=profile.password;}
  const child=spawn(binary,[path,frame],{env,stdio:['ignore','pipe','pipe']});let stdout='',stderr='';child.stdout.on('data',s=>stdout+=s);child.stderr.on('data',s=>stderr+=s);
  const timer=setTimeout(()=>child.kill('SIGKILL'),10000);const exit=await new Promise((ok,no)=>{child.on('error',no);child.on('exit',(code,signal)=>ok({code,signal}));});clearTimeout(timer);
  await writeFile(resolve(out,name+'.stdout'),stdout);await writeFile(resolve(out,name+'.stderr'),stderr);item.exit=exit;
  if(exit.code!==0) throw Error('consumer failed');
  const result=JSON.parse(stdout);if(result.received!==4 || result.codec!==codec || result.invalidSend!=='InvalidPayload') throw Error('consumer contract failed');
  const records=peers.records.filter(r=>r.digest);if(records.length!==4) throw Error('unexpected sends reached peer');
  for(let i=0;i<4;i++) {
   const expected=codec==='json'?`{"id":900719925474099312345,"sequence":${i},"text":"snow☃"}`:`sequence=${i}; snow☃`;
   if(records[i].digest!==digest(Buffer.from(expected)) || records[i].bytes!==Buffer.byteLength(expected)) throw Error('wire value differs');
   if(protocol==='mqtt' && (records[i].qos!==1 || records[i].topic!==profile.topic)) throw Error('wrong MQTT route');
   if(protocol==='ws' && records[i].binary!==(frame==='binary')) throw Error('wrong WebSocket frame');
  }
  item.consumer=result;item.status='passed';
 } catch(error) {item.status='failed';item.error=String(error);}
 finally {await writeFile(resolve(out,name+'.peer.json'),JSON.stringify(peers.records,null,2));await peers.close();report.cases.push(item);await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2));}
 console.log(JSON.stringify({name,status:item.status,error:item.error}));
}
for(const protocol of ['mqtt','ws']) for(const edition of ['2.6.0','3.0.0','3.1.0']) for(const format of ['json','yaml']) for(const codec of ['json','utf8']) await run(protocol,edition,format,codec);
await run('ws','3.1.0','yaml','json','binary');
report.status=report.cases.every(c=>c.status==='passed')?'passed':'failed';await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2));if(report.status!=='passed')process.exitCode=1;
