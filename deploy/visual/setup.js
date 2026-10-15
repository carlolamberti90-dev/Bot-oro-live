const byId=id=>document.getElementById(id);
let imageUrl=null;
async function status(){const r=await fetch('/status');if(!r.ok)throw Error('Accesso richiesto');const s=await r.json();byId('status').textContent=JSON.stringify(s,null,2);return s;}
async function frame(){const r=await fetch('/frame');if(!r.ok)throw Error('Apri il browser o ferma prima il monitor');const next=URL.createObjectURL(await r.blob());byId('screen').src=next;if(imageUrl)URL.revokeObjectURL(imageUrl);imageUrl=next;}
async function action(action,values={}){byId('message').textContent='Operazione in corso…';const r=await fetch('/action',{method:'POST',headers:{'Content-Type':'application/json','X-Monitor-Action':'1'},body:JSON.stringify({action,...values})});if(!r.ok)throw Error('Operazione non completata. Controlla lo stato.');byId('message').textContent='Operazione completata; verifica il risultato.';await status();}
function safe(fn){return async()=>{try{await fn();}catch(e){byId('message').textContent=e.message;}};}
byId('open').onclick=safe(async()=>{await action('open');await frame();});
byId('refresh').onclick=safe(frame);
byId('stop').onclick=safe(()=>action('stop'));
byId('type').onclick=safe(async()=>{const value=byId('text').value;byId('text').value='';await action('text',{text:value});await frame();});
byId('screen').onclick=async e=>{try{const box=e.currentTarget.getBoundingClientRect();await action('click',{x:(e.clientX-box.left)*1280/box.width,y:(e.clientY-box.top)*900/box.height});await frame();}catch(err){byId('message').textContent=err.message;}};
document.querySelectorAll('[data-key]').forEach(b=>b.onclick=safe(async()=>{await action('key',{key:b.dataset.key});await frame();}));
document.querySelectorAll('[data-scroll]').forEach(b=>b.onclick=safe(async()=>{await action('scroll',{dy:Number(b.dataset.scroll)});await frame();}));
byId('dry').onclick=safe(()=>action('start',{send_telegram:false}));
byId('send').onclick=safe(()=>action('start',{send_telegram:true}));
safe(status)();setInterval(safe(status),15000);
