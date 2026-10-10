import {readFile,writeFile,mkdir,mkdtemp,rm,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {resolve,dirname} from 'node:path';
import {fileURLToPath} from 'node:url';
import {createHash} from 'node:crypto';
import {execFileSync} from 'node:child_process';
const root=resolve(dirname(fileURLToPath(import.meta.url)),'../../rust/vendor/rumqttc-v4-next');
const sha=bytes=>createHash('sha256').update(bytes).digest('hex');
/** Reverse the complete local patch and verify the restored published sources. */
export async function verifyVendoredMqtt() {
 const manifest=JSON.parse(await readFile(resolve(root,'UPSTREAM.json'),'utf8'));
 const modified=new Set(manifest.modified_files);
 const temp=await mkdtemp(resolve(tmpdir(),'asyncapi-mqtt-provenance-'));
 try {
  for(const file of modified) {
   if(!Object.hasOwn(manifest.original_sha256,file) || resolve(root,file)!==root+'/'+file || file.includes('..')) throw new Error('Invalid vendor file identity');
   await mkdir(dirname(resolve(temp,file)),{recursive:true});
   await writeFile(resolve(temp,file),await readFile(resolve(root,file)));
  }
  const patch=await readFile(resolve(root,'LOCAL.patch'));
  execFileSync('git',['apply','--reverse',resolve(root,'LOCAL.patch')],{cwd:temp,stdio:'pipe'});
  for(const [file,digest] of Object.entries(manifest.original_sha256)) {
   const bytes=await readFile(resolve(modified.has(file)?temp:root,file));
   if(sha(bytes)!==digest) throw new Error('Vendored source differs beyond the declared patch: '+file);
  }
  const allowed=new Set([...Object.keys(manifest.original_sha256),'UPSTREAM.json','LOCAL.patch','LOCAL-CHANGES.md']);
  for(const file of await readdir(root,{recursive:true,withFileTypes:true})) {
   if(file.isFile()) {
    const relative=resolve(file.parentPath,file.name).slice(root.length+1);
    if(!allowed.has(relative)) throw new Error('Undeclared vendored file: '+relative);
   }
  }
  return {package:manifest.package,version:manifest.version,upstreamCommit:manifest.upstream_git,
   originalFilesVerified:Object.keys(manifest.original_sha256).length,patchSha256:sha(patch),
   stateSha256:sha(await readFile(resolve(root,'src/state.rs')))};
 } finally {await rm(temp,{recursive:true,force:true});}
}
