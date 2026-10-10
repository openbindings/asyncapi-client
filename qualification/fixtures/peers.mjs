import { Aedes } from 'aedes';
import { createServer as tcpServer } from 'node:net';
import { createServer as httpServer } from 'node:http';
import { WebSocketServer } from 'ws';
import { createHash } from 'node:crypto';

export const profile = Object.freeze({
  topic:'fixture/events',username:'fixture-user',password:'local-disposable-password',
  mqttVersion:'3.1.1',mqttBroker:'aedes@1.2.0',websocketPeer:'ws@8.22.0',
  maxRecords:10000,maxMessageBytes:1024*1024,
});
export function expectedPayload(sequence, size) {
  const value = Buffer.alloc(size);
  for(let offset=4;offset<size;offset++) value[offset]=(sequence+offset)%251;
  value.writeUInt32BE(sequence,0);
  return value;
}
export const digest = bytes => createHash('sha256').update(bytes).digest('hex');

export async function startPeers() {
  const records = [];
  let truncated = false;
  function record(value) {
    if(records.length >= profile.maxRecords) { truncated=true;return; }
    records.push(value);
  }
  const broker = await Aedes.createBroker({connectTimeout:3000,drainTimeout:3000,maxInflightInbound:32});
  broker.preConnect = (_client,packet,done) => {
    record({protocol:'mqtt',kind:'connect',client:packet.clientId,version:packet.protocolVersion,clean:packet.clean,keepalive:packet.keepalive});
    done(null,true);
  };
  broker.on('clientDisconnect',client=>record({protocol:'mqtt',kind:'disconnect',client:client.id}));
  broker.on('ping',(_packet,client)=>record({protocol:'mqtt',kind:'ping',client:client.id}));
  broker.authenticate = (_client,username,password,done) => done(null,
    username===profile.username && password?.toString()===profile.password);
  broker.on('publish',(packet,client) => {
    if(client) record({protocol:'mqtt',client:client.id,topic:packet.topic,qos:packet.qos,retain:packet.retain,
      bytes:packet.payload.length,digest:digest(packet.payload)});
  });
  broker.on('clientError',(client,error) => record({protocol:'mqtt',client:client.id,error:error.message}));
  const sockets = new Set();
  const mqtt = tcpServer(socket => { sockets.add(socket);socket.on('close',()=>sockets.delete(socket));broker.handle(socket); });
  const http = httpServer((_request,response)=>{response.writeHead(404);response.end();});
  const websocket = new WebSocketServer({server:http,path:'/events',maxPayload:profile.maxMessageBytes,perMessageDeflate:false});
  websocket.on('connection',(socket,request) => {
    record({protocol:'websocket',kind:'connect',path:request.url,method:request.method});
    socket.on('close',(code)=>record({protocol:'websocket',kind:'close',code}));
    socket.send('{"kind":"unsolicited-notice"}');
    socket.on('message',(bytes,binary) => {
      record({protocol:'websocket',binary,bytes:bytes.length,digest:digest(bytes)});
      socket.send(bytes,{binary});
    });
  });
  async function listen(server) {
    await new Promise((resolve,reject)=>{server.once('error',reject);server.listen(0,'127.0.0.1',resolve);});
    return server.address().port;
  }
  let mqttPort, websocketPort;
  try { mqttPort=await listen(mqtt);websocketPort=await listen(http); }
  catch(error) { for(const socket of sockets) socket.destroy();broker.close(()=>{});mqtt.close();http.close();throw error; }
  return { mqttPort, websocketPort, records, get truncated(){return truncated;},
    async close() {
      for(const socket of websocket.clients) socket.terminate();
      for(const socket of sockets) socket.destroy();
      await Promise.all([
        new Promise(resolve=>mqtt.close(resolve)),new Promise(resolve=>http.close(resolve)),
        new Promise(resolve=>websocket.close(resolve)),new Promise(resolve=>broker.close(resolve)),
      ]);
    },
  };
}

/** Fixed fixture expectations, never read from the candidate's prepared plan. */
export function verifyRecords(records,{protocol,count,size,topic=profile.topic}) {
  const actual = records.filter(record=>record.protocol===protocol && record.digest);
  if(actual.length!==count) throw new Error(`expected ${count} peer records, got ${actual.length}`);
  for(let sequence=0;sequence<count;sequence++) {
    const record=actual[sequence];
    if(record.bytes!==size || record.digest!==digest(expectedPayload(sequence,size))) throw new Error(`payload mismatch at ${sequence}`);
    if(protocol==='mqtt' && (record.topic!==topic || record.qos!==1 || record.retain!==false)) throw new Error(`MQTT routing/flags mismatch at ${sequence}`);
    if(protocol==='websocket' && record.binary!==true) throw new Error(`WebSocket binary frame expected at ${sequence}`);
  }
  return {observed:actual.length,bytes:size*actual.length};
}
