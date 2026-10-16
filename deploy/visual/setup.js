const byId=id=>document.getElementById(id);
let imageUrl=null;
async function status(){const r=await fetch('/status');if(!r.ok)throw Error('Accesso richiesto');const s=await r.json();byId('status').textContent=JSON.stringify(s,null,2);const ready=s.browser_open&&!s.monitor_running;byId('type').disabled=!ready;document.querySelectorAll('[data-key],[data-scroll]').forEach(b=>b.disabled=!ready);if(!s.browser_open){byId('screen').removeAttribute('src');byId('message').textContent='Il browser si è chiuso. Premi Apri TradingView prima di inserire dati.';}return s;}
async function frame(){const r=await fetch('/frame');if(!r.ok)throw Error('Apri il browser o ferma prima il monitor');const next=URL.createObjectURL(await r.blob());byId('screen').src=next;if(imageUrl)URL.revokeObjectURL(imageUrl);imageUrl=next;}
async function action(action,values={}){byId('message').textContent='Operazione in corso…';const r=await fetch('/action',{method:'POST',headers:{'Content-Type':'application/json','X-Monitor-Action':'1'},body:JSON.stringify({action,...values})});if(!r.ok)throw Error('Operazione non completata. Controlla lo stato.');byId('message').textContent='Operazione completata; verifica il risultato.';await status();}
function safe(fn){return async()=>{try{await fn();}catch(e){byId('message').textContent=e.message;}};}
byId('open').onclick=safe(async()=>{await action('open');await frame();});
byId('refresh').onclick=safe(frame);
byId('stop').onclick=safe(()=>action('stop'));
byId('type').onclick=safe(async()=>{const value=byId('text').value;if(!value)throw Error('Scrivi prima nella casella qui sopra.');await action('text',{text:value});byId('text').value='';await frame();byId('message').textContent='Testo trasferito. Premi Invio per continuare.';});
function screenPoint(image,clientX,clientY){
  const box=image.getBoundingClientRect(),width=image.naturalWidth,height=image.naturalHeight;
  if(!width||!height||!box.width||!box.height)throw Error('Aggiorna prima l’immagine.');
  const scale=Math.min(box.width/width,box.height/height);
  const left=box.left+(box.width-width*scale)/2,top=box.top+(box.height-height*scale)/2;
  const x=(clientX-left)/scale,y=(clientY-top)/scale;
  if(x<0||y<0||x>=width||y>=height)return null;
  return {x,y};
}
byId('screen').onclick=async e=>{try{const point=screenPoint(e.currentTarget,e.clientX,e.clientY);if(!point)return;await action('click',point);await frame();byId('text').scrollIntoView({block:'center'});byId('text').focus({preventScroll:true});byId('message').textContent='Campo selezionato. Scrivi qui e premi Trasferisci al browser.';}catch(err){byId('message').textContent=err.message;}};
document.querySelectorAll('[data-key]').forEach(b=>b.onclick=safe(async()=>{await action('key',{key:b.dataset.key});await frame();}));
document.querySelectorAll('[data-scroll]').forEach(b=>b.onclick=safe(async()=>{await action('scroll',{dy:Number(b.dataset.scroll)});await frame();}));
byId('dry').onclick=safe(()=>action('start',{send_telegram:false}));
byId('send').onclick=safe(()=>action('start',{send_telegram:true}));
safe(status)();setInterval(safe(status),15000);
