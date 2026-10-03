const WebSocket=require('ws'); process.env.PORT=8099; process.env.INTRO_MS=150; process.env.RESULT_MS=150; process.env.QUICK_WAIT_MS=300; process.env.DATA_DIR='/tmp/stdata'+Date.now();
const {server,periodKey}=require('./server.js');
const sleep=ms=>new Promise(r=>setTimeout(r,ms)); let fails=0; const ok=(c,m)=>{ console.log((c?'OK   ':'FAIL ')+m); if(!c) fails++; };
function client(name){ return new Promise(res=>{ const ws=new WebSocket('ws://localhost:8099'); const c={ws,name,msgs:[],last:{},snaps:0,chat:[]};
  ws.on('message',r=>{ const m=JSON.parse(r); c.msgs.push(m); c.last[m.t]=m; if(m.t==='snap') c.snaps++; if(m.t==='chat') c.chat.push(m); });
  c.send=(t,d={})=>ws.send(JSON.stringify({t,...d})); ws.on('open',()=>{ c.send('hello',{deviceId:'dev-'+name,name}); setTimeout(()=>res(c),60); }); }); }
(async()=>{
  const [A,B,C,D]=await Promise.all(['Ali','Berk','Cem','Deniz'].map(client));
  ok(A.last.welcome&&A.last.welcome.name==='Ali','hello/welcome');
  A.send('create',{name:'Test Kupası',max:4,diff:'NORMAL'}); await sleep(80);
  const room=A.last.room.room; ok(room&&room.hostId===A.last.welcome.id,'oda kuruldu, kurucu Ali, kod '+room.code);
  B.send('rooms'); await sleep(60); ok(B.last.rooms.list.some(r=>r.code===room.code),'açık turnuva listesinde görünüyor');
  B.send('join',{code:room.code}); C.send('join',{roomId:room.id}); D.send('join',{code:room.code}); await sleep(80);
  ok(B.last.pending&&!B.last.room,'Berk onay bekliyor (odaya girmedi)'); ok(A.last.room.room.pending.length===3,'kurucu 3 isteği görüyor');
  B.send('start'); await sleep(50); ok(B.last.err&&B.last.err.code==='nothost','kurucu olmayan başlatamaz');
  A.send('approve',{id:B.last.welcome.id}); A.send('approve',{id:C.last.welcome.id}); A.send('reject',{id:D.last.welcome.id}); await sleep(80);
  ok(B.last.room&&B.last.room.room.members.length===3,'Berk ve Cem onaylandı (3 kişi)'); ok(D.last.rejected,'Deniz reddedildi');
  B.send('chat',{text:'merhaba <b>herkes</b>'}); await sleep(60); ok(A.chat.some(m=>m.text&&m.text.includes('merhaba')),'sohbet iletildi');
  A.send('start'); await sleep(250);
  let rm=A.last.room.room; ok(rm.phase==='playing'&&rm.turn&&rm.turn.phase==='live','oyun başladı, ilk atış canlı');
  // sırayla 9 atış
  const cl={[A.last.welcome.id]:A,[B.last.welcome.id]:B,[C.last.welcome.id]:C}; const seq=[];
  for(let k=0;k<9;k++){
    for(let w=0;w<40;w++){ rm=A.last.room.room; if(rm.turn&&rm.turn.phase==='live'&&rm.turn.idx===k) break; await sleep(20); }
    const sh=cl[rm.turn.shooter]; seq.push(sh.name+rm.turn.shotNo);
    const before={A:A.snaps,B:B.snaps,C:C.snaps}; sh.send('snap',{d:{s:'SEARCHING'}}); await sleep(40);
    const others=Object.values(cl).filter(x=>x!==sh); if(k===0) ok(others.every(o=>o.snaps>before[o.name[0]]) && sh.snaps===before[sh.name[0]],'canlı görüntü izleyicilere aktarıldı (atıcıya geri gelmedi)');
    const wrong=others[0]; wrong.send('result',{score:999,i:k}); 
    sh.send('result',{score:k===4?999:50+k,hit:k%2===0,miss:k%2?20:null,kind:'shot',i:k}); await sleep(30);
  }
  await sleep(300);
  ok(seq.join(' ')==='Ali1 Berk1 Cem1 Ali2 Berk2 Cem2 Ali3 Berk3 Cem3','sıra: '+seq.join(' '));
  const over=A.last.over; ok(over&&over.ranking.length===3,'oyun bitti, sıralama geldi');
  const berk=over.ranking.find(x=>x.name==='Berk'); ok(berk.total===(51)+(210)+(57),'puan sunucuda sınırlandı (999→210), Berk toplam '+berk.total);
  ok(over.ranking.every(x=>x.total===x.total),'başkasının sonucu kabul edilmedi');
  // liderlik
  A.send('submit',{score:420,hits:3,shots:3,diff:'HARD',key:'m1'}); A.send('submit',{score:420,key:'m1'}); B.send('submit',{score:300,hits:2,shots:3,key:'b1'}); await sleep(60);
  for(const per of ['day','week','month']){ A.send('lb',{period:per,kind:'solo'}); await sleep(50); const lb=A.last.lb; ok(lb.rows[0].name==='Ali'&&lb.rows[0].score===420&&lb.rows.length===2&&lb.me.rank===1,`${per} liderlik (${lb.key}): `+lb.rows.map(r=>r.name+' '+r.score).join(', ')); }
  A.send('lb',{period:'week',kind:'tour'}); await sleep(50); ok(A.last.lb.rows.length===3,'turnuva liderliği otomatik kaydedildi');
  ok(periodKey(Date.UTC(2026,0,1,22,0),'day')==='2026-1-2','Türkiye saati ile gün dönümü');
  // hızlı maç
  A.send('leave'); B.send('leave'); C.send('leave'); await sleep(50);
  C.send('quick'); D.send('quick'); await sleep(500);
  ok(C.last.quick.state==='matched'&&D.last.room&&D.last.room.room.quick,'hızlı maç eşleşti (onaysız)');
  await sleep(3400); ok(D.last.room.room.phase==='playing','hızlı maç otomatik başladı');
  // atıcı bağlantı koparsa
  rm=D.last.room.room; const shooter=rm.turn.shooter===C.last.welcome.id?C:D, other=shooter===C?D:C;
  for(let w=0;w<30&&D.last.room.room.turn.phase!=='live';w++) await sleep(20);
  shooter.ws.close(); await sleep(150); const t=other.last.room.room; const res=t.members.find(m=>m.name===shooter.name).shots[0];
  ok(res&&res.kind==='offline'&&res.score===0,'bağlantısı kopan atıcıya 0 puan, oyun devam etti');
  console.log(fails?`\n${fails} HATA`:'\nTÜM SUNUCU TESTLERİ GEÇTİ'); process.exit(fails?1:0);
})();
