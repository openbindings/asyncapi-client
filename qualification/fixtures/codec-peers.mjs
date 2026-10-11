import {createServer} from 'node:http';
import {WebSocketServer} from 'ws';
import {digest} from './peers.mjs';
export async function startCodecPeers() {
 const server=createServer(),ws=new WebSocketServer({server,maxPayload:1024*1024,perMessageDeflate:false}),records=[];
 ws.on('connection',(socket,request)=>{
  const path=request.url, binary=path.endsWith('-binary'),json=path.startsWith('/json-');
  records.push({kind:'connect',path});
  socket.send('{}',{binary:!binary});
  if(json)socket.send('{',{binary});
  else if(binary)socket.send(Buffer.from([255]),{binary:true});
  socket.on('message',(data,isBinary)=>{records.push({path,binary:isBinary,bytes:data.length,digest:digest(data)});socket.send(data,{binary:isBinary});});
  socket.on('close',code=>records.push({kind:'close',path,code}));
 });
 await new Promise((ok,no)=>{server.once('error',no);server.listen(0,'127.0.0.1',ok);});
 return {port:server.address().port,records,async close(){for(const socket of ws.clients)socket.terminate();await Promise.all([new Promise(ok=>ws.close(ok)),new Promise(ok=>server.close(ok))]);}};
}
export function verifyCodecRecords(records) {
 const actual=records.filter(r=>r.digest),expected=[];
 for(const codec of ['json','utf8'])for(const frame of ['text','binary']) {
  for(const payload of codec==='json'?['{"id":7}','{"id":900719925474099312345}']:['snow☃','']) expected.push({path:`/${codec}-${frame}`,binary:frame==='binary',bytes:Buffer.byteLength(payload),digest:digest(Buffer.from(payload))});
 }
 for(const frame of ['text','binary']) {const payload='{"id":900719925474099312345}';expected.push({path:`/json-${frame}`,binary:frame==='binary',bytes:Buffer.byteLength(payload),digest:digest(Buffer.from(payload))});}
 if(JSON.stringify(actual)!==JSON.stringify(expected))throw Error('codec peer observed different frames, payload bytes, ordering or send count');
 return {observed:actual.length,expected:expected.length};
}
