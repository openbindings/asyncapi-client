import {execFile} from 'node:child_process';
import {readFile,writeFile,mkdir} from 'node:fs/promises';
import {dirname,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {startHostFaults} from './host-faults.mjs';
const here=dirname(fileURLToPath(import.meta.url)),repo=resolve(here,'../..'),out=resolve(process.argv[2]??'');
if(!process.argv[2])throw Error('Usage: node check-query-auth.mjs FRESH_OUTPUT_DIRECTORY');
await mkdir(dirname(out),{recursive:true});await mkdir(out);
const hash=bytes=>createHash('sha256').update(bytes).digest('hex'),binary=resolve(repo,'rust/target/debug/examples/exchange');
const cases=['2.6.0','3.0.0','3.1.0'].flatMap(edition=>['valid','second-identity','tls-valid','tls-composed','missing','missing-operation-key','unused','wrong','redirect'].map(mode=>({edition,mode}))).concat([{edition:'3.1.0',mode:'mqtt-unrelated'}]);
const report={classification:'development query authentication peers',independent:false,startedAt:new Date().toISOString(),binarySha256:hash(await readFile(binary)),sourceHashes:{},cases:[]};
for(const name of ['check-query-auth.mjs','host-faults.mjs','documents/ws-2.json','documents/ws-3.json','documents/mqtt-3.json'])report.sourceHashes[name]=hash(await readFile(resolve(here,name)));
for(const name of ['server.pem','server.key.pem','fixture-ca.pem','client.pem','client.key.pem'])report.sourceHashes['tls/'+name]=hash(await readFile(resolve(repo,'rust/native/tests/fixtures/tls',name)));
await writeFile(resolve(out,'plan.json'),JSON.stringify({cases,expectations:'Valid credentials exchange twice. Missing/unused keys and MQTT query configuration refuse with no TCP connection. Wrong value and redirects fail readiness, with no messages and no redirect-target request. Request values are recorded only as hashes.'},null,2));
const check=(v,m)=>{if(!v)throw Error(m);};
for(const {edition,mode} of cases) {
 const name=edition+'-'+mode,certs=resolve(repo,'rust/native/tests/fixtures/tls'),secure=mode.startsWith('tls-'),composed=mode==='tls-composed';
 const tls=secure?{cert:await readFile(resolve(certs,'server.pem')),key:await readFile(resolve(certs,'server.key.pem')),ca:await readFile(resolve(certs,'fixture-ca.pem')),requestCert:composed,rejectUnauthorized:true}:undefined;
 const peer=await startHostFaults(tls),item={name,edition,mode};
 try {
  const doc=JSON.parse(await readFile(resolve(here,'documents/'+(mode==='mqtt-unrelated'?'mqtt-3.json':edition==='2.6.0'?'ws-2.json':'ws-3.json')),'utf8'));
  const path=mode==='redirect'?'/auth-redirect':composed?'/auth-certificate':'/auth';doc.asyncapi=edition;doc.servers.local[edition==='2.6.0'?'url':'host']='127.0.0.1:'+peer.port;
  if(mode!=='mqtt-unrelated') {
   doc.components={securitySchemes:{query:{type:'httpApiKey',in:'query',name:'access key+雪'},scope:{type:'httpApiKey',in:'query',name:'scope'}}};
   if(edition==='2.6.0') {doc.channels={[path]:Object.values(doc.channels)[0]};doc.servers.local.security=[{query:[],scope:[]}];}
   else {doc.channels.events.address=path;doc.servers.local.security=[{$ref:'#/components/securitySchemes/query'}];doc.operations.emit.security=[{$ref:'#/components/securitySchemes/scope'}];}
  }
  if(secure)doc.servers.local.protocol='wss';
  if(composed){doc.components.securitySchemes.cert={type:'X509'};if(edition==='2.6.0')doc.servers.local.security=[{query:[],cert:[]}];else {doc.servers.local.security=[{$ref:'#/components/securitySchemes/cert'}];doc.operations.emit.security=[{$ref:'#/components/securitySchemes/query'}];}}
  const document=resolve(out,name+'.json');await writeFile(document,JSON.stringify(doc));
  const credentials={'access key+雪':mode==='wrong'?'incorrect':mode==='second-identity'?'different=雪':'v&=+?/雪#% ',scope:'events&read'};
  if(mode==='missing-operation-key'||composed)delete credentials.scope;
  if(mode==='unused')credentials.unrelated='private';
  const env={...process.env};for(const key of ['ASYNCAPI_FIXTURE_CA','ASYNCAPI_FIXTURE_CERT','ASYNCAPI_FIXTURE_KEY','ASYNCAPI_FIXTURE_USERNAME','ASYNCAPI_FIXTURE_PASSWORD','ASYNCAPI_FIXTURE_QUERY_CREDENTIALS'])delete env[key];
  if(secure)env.ASYNCAPI_FIXTURE_CA=resolve(certs,'fixture-ca.pem');
  if(composed){env.ASYNCAPI_FIXTURE_CERT=resolve(certs,'client.pem');env.ASYNCAPI_FIXTURE_KEY=resolve(certs,'client.key.pem');}
  if(mode!=='missing')env.ASYNCAPI_FIXTURE_QUERY_CREDENTIALS=JSON.stringify(credentials);
  const result=await new Promise(yes=>execFile(binary,[document,'emit','listen','2','64'],{env,timeout:8000,maxBuffer:128*1024},(error,stdout,stderr)=>yes({code:error?.code??0,killed:error?.killed??false,stdout,stderr})));
  await writeFile(resolve(out,name+'.stdout'),result.stdout);await writeFile(resolve(out,name+'.stderr'),result.stderr);item.exitCode=result.code;check(!result.killed,'consumer timed out');
  check(!result.stdout.includes('v&=+?/雪#% ')&&!result.stderr.includes('v&=+?/雪#% '),'credential leaked into consumer output');
  const queries=peer.records.filter(r=>r.kind==='query-auth'),messages=peer.records.filter(r=>r.kind==='auth-message');
  if(['valid','second-identity','tls-valid','tls-composed'].includes(mode)) {
   check(result.code===0,'expected success: '+result.stderr);check(queries.length===1 && queries[0].valid,'peer did not verify credentials');check(queries[0].valueSha256===hash(credentials['access key+雪']),'peer observed wrong credential');check(messages.length===2 && messages.every(r=>r.binary&&r.bytes===64),'peer did not observe binary exchange');
   if(secure)check(peer.records.some(r=>r.kind==='tls'),'secure query did not use TLS');
   if(composed)check(peer.records.some(r=>r.kind==='tls'&&r.clientAuthorized&&r.client==='fixture-client'),'composed client certificate was not verified');
  } else {
   check(result.code!==0,'expected refusal');const preflight=['missing','missing-operation-key','unused','mqtt-unrelated'].includes(mode);
   check(result.stderr.includes('code: '+(preflight?'InvalidConfiguration':'Connection')),'wrong failure: '+result.stderr);
   check(messages.length===0,'refusal published messages');
   if(preflight)check(!peer.records.some(r=>r.kind==='tcp'),'invalid configuration opened a socket');
   else check(queries.length===1,'expected exactly one authentication request');
   check(!peer.records.some(r=>r.path==='/auth-leak'),'client followed authenticated redirect');
  }
  item.status='passed';
 }catch(error){item.status='failed';item.error=String(error.stack??error);}
 finally {item.records=peer.records;await peer.close();report.cases.push(item);await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2));}
}
report.status=report.cases.every(c=>c.status==='passed')?'passed':'failed';await writeFile(resolve(out,'report.json'),JSON.stringify(report,null,2)+'\n');
console.log(JSON.stringify({out,status:report.status,cases:report.cases.length,failures:report.cases.filter(c=>c.status==='failed').map(c=>({name:c.name,error:c.error}))}));if(report.status!=='passed')process.exitCode=1;
