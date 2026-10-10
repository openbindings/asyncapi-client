export async function exerciseCodecs(client,outerCodec,source) {
 const observations=[];
 function check(name,actual,expected) {if(JSON.stringify(actual)!==JSON.stringify(expected))throw Error(`${name}: ${JSON.stringify(actual)} != ${JSON.stringify(expected)}`);observations.push({name,actual,expected});}
 function documentFor(codec,frame) {
  const doc=JSON.parse(source);doc.channels.events.address=`/${codec}-${frame}`;
  // Locate the native message in this fixed 3.x fixture.
  for(const message of Object.values(doc.channels.events.messages))message.contentType=codec==='json'?'application/json':'text/plain; charset=utf-8';
  return JSON.stringify(doc);
 }
 for(const codec of ['json','utf8'])for(const frame of ['text','binary']) {
  const label=`${codec}-${frame}`,document=client.parse(documentFor(codec,frame));
  const plans=['emit','listen'].map(id=>{const op=document.operation(id),compiled=op.compile();try{return compiled.prepare({role:'application',websocketFrame:frame});}finally{op.dispose();compiled.dispose();}});
  document.dispose();const session=await client.openSession(plans,{limits:{maxMessageBytes:32}});plans.forEach(p=>p.dispose());const sender=session.sender();let retained;
  try {
   let invalid;
   try {sender.send(0,new Uint8Array([255]));}catch(error){invalid=error.code;}
   check(`${label}-invalid-send`,invalid,'InvalidPayload');
   let surrogate;
   try {sender.sendText(0,'\ud800');}catch(error){surrogate=error.code;}
   check(`${label}-unicode-send`,surrogate,'InvalidPayload');
   if(codec==='utf8') {
    const before=session.usage;let tooLarge;
    try{sender.sendText(0,'😀'.repeat(9));}catch(error){tooLarge=error.code;}
    check(`${label}-utf8-byte-limit`,tooLarge,'Backpressure');
    check(`${label}-refusal-releases-capacity`,session.usage,before);
   }
   if(codec==='json') {
    sender.sendValue(0,{id:7});const exact=client.parseJson('{"id":900719925474099312345}');try{sender.sendJson(0,exact);}finally{exact.dispose();}
   } else {sender.sendText(0,'snow☃');sender.sendText(0,'');}
   const values=[];let rejected=0,malformed=0;const malformedCodes=[];
   while(values.length<2) {
    const next=await session.next({signal:AbortSignal.timeout(3000)});
    if(next?.kind==='rejected'){rejected++;continue;}
    if(next?.kind==='invalidPayload'){malformed++;malformedCodes.push(next.diagnostic.code);continue;}
    if(next?.kind!=='message'||next.operation!==1||next.codec!==codec)throw Error('wrong decoded receive type');
    if(codec==='json') {const id=next.value.get('id');values.push(id.numberText);next.value.dispose();retained?.dispose();retained=id;}
    else values.push(next.text);
   }
   check(`${label}-decoded-values`,values,codec==='json'?['7','900719925474099312345']:['snow☃','']);
   check(`${label}-wrong-frame`,rejected,1);check(`${label}-malformed-body`,malformed,codec==='json'||frame==='binary'?1:0);
   check(`${label}-malformed-code`,malformedCodes,codec==='json'?['InvalidJson']:frame==='binary'?['InvalidValue']:[]);
   await session.close();
   if(retained)check(`${label}-retained-value-after-close`,retained.numberText,'900719925474099312345');
  } finally {retained?.dispose();sender.dispose();session.dispose();}
 }
 for(const frame of ['text','binary']) {
  const result=JSON.parse(await outerCodec(documentFor('json',frame),frame==='binary'));
  check(`direct-Rust-json-${frame}-fields`,Object.keys(result).sort(),['id','malformed','rejected']);
  check(`direct-Rust-json-${frame}`,[result.id,result.rejected,result.malformed],['900719925474099312345',1,1]);
 }
 return {status:'passed',observations,claim:'development codec exchanges, exact ownership and malformed-message progress; schema evaluation not claimed'};
}
