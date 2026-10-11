import {createServer} from 'node:http';
import {createServer as createSecureServer} from 'node:https';
import {WebSocketServer} from 'ws';
import {createHash} from 'node:crypto';
export async function startHostFaults(tlsOptions) {
 const records=[],sockets=new Set(),handler=(_q,r)=>{r.writeHead(404);r.end();};const server=tlsOptions?createSecureServer(tlsOptions,handler):createServer(handler);
 if(tlsOptions)server.on('secureConnection',socket=>records.push({kind:'tls',clientAuthorized:socket.authorized,client:socket.getPeerCertificate().subject?.CN??null}));
 server.on('connection',socket=>{records.push({kind:'tcp'});sockets.add(socket);socket.on('close',()=>sockets.delete(socket));});
 const websocket=new WebSocketServer({noServer:true,perMessageDeflate:false});
 server.on('upgrade',(request,socket,head)=>{
  const url=new URL(request.url,'http://fixture.test'),path=url.pathname;
  records.push({kind:'upgrade',path});
  if(path.startsWith('/auth')) {
   const entries=[...url.searchParams],value=url.searchParams.get('access key+雪');
   const valid=entries.length===(path==='/auth-certificate'?1:2) && url.searchParams.getAll('access key+雪').length===1 && ['v&=+?/雪#% ','different=雪'].includes(value) && (path==='/auth-certificate' || (url.searchParams.getAll('scope').length===1 && url.searchParams.get('scope')==='events&read'));
   records.push({kind:'query-auth',path,valid,names:entries.map(([name])=>name),valueSha256:createHash('sha256').update(value??'').digest('hex')});
   if(path==='/auth-redirect') {socket.end('HTTP/1.1 302 Found\r\nLocation: ws://127.0.0.1:'+server.address().port+'/auth-leak'+url.search+'\r\nContent-Length: 0\r\nConnection: close\r\n\r\n');return;}
   if(!valid||path==='/auth-leak') {socket.end('HTTP/1.1 403 Forbidden\r\nContent-Length: 0\r\nConnection: close\r\n\r\n');return;}
   websocket.handleUpgrade(request,socket,head,ws=>{
    ws.on('error',()=>records.push({kind:'auth-peer-error'}));ws.on('close',code=>records.push({kind:'auth-close',code}));
    ws.on('message',(data,binary)=>{records.push({kind:'auth-message',binary,bytes:data.length});ws.send(data,{binary});});
   });return;
  }
  if(path==='/stall')return;
  websocket.handleUpgrade(request,socket,head,ws=>{
   ws.on('error',error=>records.push({kind:'peer-error',path:request.url,message:error.message}));
   ws.on('close',code=>records.push({kind:'close',path:request.url,code}));
   if(request.url==='/stream-echo') {
    ws.on('message',(data,isBinary)=>{records.push({kind:'stream-echo',binary:isBinary,bytes:[...data]});ws.send(data,{binary:isBinary});});
    return;
   }
   ws.once('message',()=>{
    records.push({kind:'armed',path:request.url});
    switch(request.url) {
     case '/count': for(let i=0;i<3;i++)ws.send(Buffer.from([i]));break;
     case '/bytes': ws.send(Buffer.alloc(5,1));ws.send(Buffer.alloc(5,2));break;
     case '/large': ws.send(Buffer.alloc(9,3));break;
     case '/text': ws.send('é🙂');break;
     case '/abnormal': ws.terminate();break;
     case '/no-close': ws.send(Buffer.from([0]));ws._socket.pause();break;
     case '/stream-close': ws.send(Buffer.from([42]));ws.close(1000);break;
     default: break;
    }
   });
  });
 });
 await new Promise((r,j)=>{server.once('error',j);server.listen(0,'127.0.0.1',r);});
 return {port:server.address().port,records,async close(){for(const socket of sockets)socket.destroy();for(const ws of websocket.clients)ws.terminate();await Promise.all([new Promise(r=>server.close(r)),new Promise(r=>websocket.close(r))]);}};
}
