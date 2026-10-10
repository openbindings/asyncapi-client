import {createServer} from 'node:http';
import {WebSocketServer} from 'ws';
export async function startHostFaults() {
 const records=[],sockets=new Set();const server=createServer((_q,r)=>{r.writeHead(404);r.end();});
 server.on('connection',socket=>{sockets.add(socket);socket.on('close',()=>sockets.delete(socket));});
 const websocket=new WebSocketServer({noServer:true,perMessageDeflate:false});
 server.on('upgrade',(request,socket,head)=>{
  records.push({kind:'upgrade',path:request.url});
  if(request.url==='/stall')return;
  websocket.handleUpgrade(request,socket,head,ws=>{
   ws.on('error',error=>records.push({kind:'peer-error',path:request.url,message:error.message}));
   ws.on('close',code=>records.push({kind:'close',path:request.url,code}));
   ws.once('message',()=>{
    records.push({kind:'armed',path:request.url});
    switch(request.url) {
     case '/count': for(let i=0;i<3;i++)ws.send(Buffer.from([i]));break;
     case '/bytes': ws.send(Buffer.alloc(5,1));ws.send(Buffer.alloc(5,2));break;
     case '/large': ws.send(Buffer.alloc(9,3));break;
     case '/text': ws.send('é🙂');break;
     case '/abnormal': ws.terminate();break;
     case '/no-close': ws.send(Buffer.from([0]));ws._socket.pause();break;
     default: break;
    }
   });
  });
 });
 await new Promise((r,j)=>{server.once('error',j);server.listen(0,'127.0.0.1',r);});
 return {port:server.address().port,records,async close(){for(const socket of sockets)socket.destroy();for(const ws of websocket.clients)ws.terminate();await Promise.all([new Promise(r=>server.close(r)),new Promise(r=>websocket.close(r))]);}};
}
