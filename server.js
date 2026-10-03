'use strict';
/* =====================================================================
   STINGER – Interceptor Challenge · Online sunucu
   Liderlik panoları (günlük/haftalık/aylık), hızlı maç, turnuva odaları
   (kurucu onayı), sıralı atış yönetimi, canlı izleme rölesi, sohbet.
   Çalıştırma:  npm install  &&  npm start      (PORT, DATA_DIR ortam değişkenleri)
   ===================================================================== */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = +process.env.PORT || 8080;
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const PUBLIC_DIR = path.join(__dirname, 'public');
const VERSION = '1.1.0';

const CFG = {
  shotsPerPlayer: 3,
  introMs: +process.env.INTRO_MS || 4500,      // sıra geçiş kartı
  resultMs: +process.env.RESULT_MS || 4500,    // sonuç kartı
  liveMs: +process.env.LIVE_MS || 85000,       // bir atışın azami süresi (45 sn + uçuş + pay)
  quickWaitMs: +process.env.QUICK_WAIT_MS || 8000,
  quickMax: 4, roomMaxLimit: 8, offlineGraceMs: 60000,
  maxShotScore: 210,      // isabet 100 + süre 20 + kilit 10 = 130, UZMAN ×1.6 = 208
  maxSoloScore: 1500, keepDays: 40,
};

/* ---------------- kalıcı kayıt (liderlik) ----------------
   Upstash Redis (ücretsiz) ortam değişkenleri verilirse kayıtlar orada tutulur ve sunucu
   yeniden başlasa da kaybolmaz. Verilmezse DATA_DIR içindeki scores.json kullanılır.        */
fs.mkdirSync(DATA_DIR, { recursive: true });
const SCORES_FILE = path.join(DATA_DIR, 'scores.json');
const UP_URL = (process.env.UPSTASH_REDIS_REST_URL || '').replace(/\/+$/, '');
const UP_TOK = process.env.UPSTASH_REDIS_REST_TOKEN || '';
const UP_KEY = process.env.SCORES_KEY || 'stinger:scores';
const useUpstash = !!(UP_URL && UP_TOK);
let scores = [];
try { scores = JSON.parse(fs.readFileSync(SCORES_FILE, 'utf8')); if (!Array.isArray(scores)) scores = []; } catch (e) { scores = []; }
let storageReady = Promise.resolve();
if (useUpstash) {
  storageReady = fetch(`${UP_URL}/get/${encodeURIComponent(UP_KEY)}`, { headers: { Authorization: `Bearer ${UP_TOK}` } })
    .then(r => r.json()).then(j => {
      if (j && typeof j.result === 'string') { const a = JSON.parse(j.result); if (Array.isArray(a)) { const seen = new Set(a.map(x => x.pid + x.ts)); scores = a.concat(scores.filter(x => !seen.has(x.pid + x.ts))); } }
      console.log(`Upstash bağlı · ${scores.length} liderlik kaydı yüklendi`);
    }).catch(e => console.error('Upstash okunamadı, dosya kaydıyla devam:', e.message));
}
let saveTimer = null;
function saveScores() {
  if (saveTimer) return;
  saveTimer = setTimeout(() => {
    saveTimer = null;
    const cutoff = Date.now() - CFG.keepDays * 864e5;
    scores = scores.filter(s => s.ts >= cutoff);
    const body = JSON.stringify(scores);
    const tmp = SCORES_FILE + '.tmp';
    try { fs.writeFileSync(tmp, body); fs.renameSync(tmp, SCORES_FILE); } catch (e) { console.error('dosya kayıt hatası', e.message); }
    if (useUpstash) fetch(`${UP_URL}/set/${encodeURIComponent(UP_KEY)}`, { method: 'POST', headers: { Authorization: `Bearer ${UP_TOK}` }, body })
      .then(r => { if (!r.ok) console.error('Upstash yazma hatası', r.status); }).catch(e => console.error('Upstash yazma hatası', e.message));
  }, 1500);
}
// Türkiye saati (UTC+3) ile dönem anahtarları
function trDate(ts) { return new Date(ts + 3 * 3600e3); }
function periodKey(ts, period) {
  const d = trDate(ts), y = d.getUTCFullYear(), m = d.getUTCMonth() + 1, day = d.getUTCDate();
  if (period === 'day') return `${y}-${m}-${day}`;
  if (period === 'month') return `${y}-${m}`;
  const t = new Date(Date.UTC(y, m - 1, day)); const wd = (t.getUTCDay() + 6) % 7; t.setUTCDate(t.getUTCDate() - wd + 3);
  const fy = t.getUTCFullYear(), jan4 = new Date(Date.UTC(fy, 0, 4));
  return `${fy}-W${1 + Math.round(((t - jan4) / 864e5 - 3 + ((jan4.getUTCDay() + 6) % 7)) / 7)}`;
}
function leaderboard(period, kind, pid) {
  const now = Date.now(), key = periodKey(now, period), best = new Map();
  for (const s of scores) {
    if (s.kind !== kind || periodKey(s.ts, period) !== key) continue;
    const b = best.get(s.pid);
    if (!b || s.score > b.score || (s.score === b.score && s.ts < b.ts)) best.set(s.pid, s);
  }
  const rows = [...best.values()].sort((a, b) => b.score - a.score || a.ts - b.ts);
  const out = rows.slice(0, 50).map((s, i) => ({ rank: i + 1, name: s.name, score: s.score, hits: s.hits, shots: s.shots, diff: s.diff, me: s.pid === pid }));
  const mi = rows.findIndex(s => s.pid === pid);
  return { period, kind, key, rows: out, me: mi >= 0 ? { rank: mi + 1, score: rows[mi].score } : null, total: rows.length };
}
function addScore(p, kind, score, hits, shots, diff) {
  scores.push({ pid: p.pid, name: p.name, kind, score, hits, shots, diff, ts: Date.now() });
  saveScores();
}

/* ---------------- yardımcılar ---------------- */
const uid = (n = 8) => crypto.randomBytes(n).toString('base64url').slice(0, n);
const CODE_CHARS = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
function roomCode() { let c; do { c = ''; for (let i = 0; i < 5; i++) c += CODE_CHARS[crypto.randomInt(CODE_CHARS.length)]; } while ([...rooms.values()].some(r => r.code === c)); return c; }
function cleanName(s) { s = String(s || '').replace(/[\u0000-\u001f<>]/g, '').replace(/\s+/g, ' ').trim().slice(0, 16); return s.length >= 2 ? s : null; }
function cleanText(s, n) { return String(s || '').replace(/[\u0000-\u001f]/g, ' ').trim().slice(0, n); }
const clampInt = (v, a, b) => Math.max(a, Math.min(b, Math.round(+v || 0)));
const DIFFS = ['NORMAL', 'HARD', 'EXPERT'];

/* ---------------- oyuncular ---------------- */
const players = new Map();   // pid -> player
const byDevice = new Map();  // deviceId -> pid
function send(p, t, d = {}) { if (p && p.ws && p.ws.readyState === 1) p.ws.send(JSON.stringify({ t, ...d })); }
function sendRaw(p, s) { if (p && p.ws && p.ws.readyState === 1 && p.ws.bufferedAmount < 1e6) p.ws.send(s); }
const onlineCount = () => [...players.values()].filter(p => p.ws).length;

/* ---------------- odalar ---------------- */
const rooms = new Map();
let quickQueue = [], quickTimer = null;

function newRoom({ name, hostId, max, diff, quick }) {
  const r = { id: uid(8), code: roomCode(), name, hostId, max, diff, quick: !!quick, phase: 'lobby', members: [], pending: [], order: [],
    results: {}, idx: 0, turn: null, timer: null, chat: [], created: Date.now(), game: 0 };
  rooms.set(r.id, r); return r;
}
function roomPublic(r, forPid) {
  const totals = r.order.length ? r.order : r.members;
  const mem = [...new Set([...r.members, ...totals])].map(id => {
    const p = players.get(id); const res = r.results[id] || [];
    return { id, name: p ? p.name : '?', online: !!(p && p.ws), inRoom: r.members.includes(id), host: id === r.hostId,
      shots: res, total: res.reduce((a, s) => a + s.score, 0) };
  });
  return { id: r.id, code: r.code, name: r.name, hostId: r.hostId, max: r.max, diff: r.diff, quick: r.quick, phase: r.phase, game: r.game,
    members: mem, order: r.order, turn: r.turn && { ...r.turn, now: Date.now() },
    pending: forPid === r.hostId ? r.pending.map(id => ({ id, name: (players.get(id) || {}).name || '?' })) : [], shotsPerPlayer: CFG.shotsPerPlayer };
}
function broadcastRoom(r) { for (const id of r.members) send(players.get(id), 'room', { room: roomPublic(r, id) }); }
function roomSys(r, key, args = {}) { const m = { sys: key, args, ts: Date.now() }; r.chat.push(m); if (r.chat.length > 60) r.chat.shift(); for (const id of r.members) send(players.get(id), 'chat', m); }
function relay(r, fromId, str) { for (const id of r.members) if (id !== fromId) sendRaw(players.get(id), str); }
function openRooms() {
  return [...rooms.values()].filter(r => !r.quick && r.phase === 'lobby').map(r => ({ id: r.id, code: r.code, name: r.name, host: (players.get(r.hostId) || {}).name || '?',
    count: r.members.length, max: r.max, diff: r.diff }));
}
function removeFromRoom(p, why) {
  const r = rooms.get(p.room); p.room = null; if (!r) return;
  r.pending = r.pending.filter(id => id !== p.pid);
  if (!r.members.includes(p.pid)) return;
  r.members = r.members.filter(id => id !== p.pid);
  roomSys(r, why === 'kick' ? 'kicked' : 'left', { name: p.name });
  if (r.hostId === p.pid) { r.hostId = r.quick ? null : (r.members[0] || null); if (r.hostId) roomSys(r, 'newhost', { name: players.get(r.hostId).name }); }
  if (!r.members.length) { clearTimeout(r.timer); rooms.delete(r.id); return; }
  if (r.phase === 'playing') {
    if (r.turn && r.turn.shooter === p.pid && r.turn.phase !== 'result') finishTurn(r, { score: 0, kind: 'left' });
    if (r.members.length < 1) endGame(r);
  }
  broadcastRoom(r);
}
function startGame(r) {
  clearTimeout(r.timer);
  r.phase = 'playing'; r.game++; r.order = r.members.slice(); r.results = {}; for (const id of r.order) r.results[id] = [];
  r.idx = 0; r.pending.forEach(id => { const q = players.get(id); if (q) { q.room = null; send(q, 'rejected', { name: r.name, why: 'started' }); } }); r.pending = [];
  roomSys(r, 'started', { n: r.order.length });
  nextTurn(r);
}
function nextTurn(r) {
  clearTimeout(r.timer);
  const n = r.order.length, total = n * CFG.shotsPerPlayer;
  while (r.idx < total) {
    const shooter = r.order[r.idx % n];
    if (r.members.includes(shooter)) break;
    r.results[shooter].push({ score: 0, kind: 'left', hit: false }); r.idx++;   // ayrılan oyuncunun hakkı 0
  }
  if (r.idx >= total || !r.members.length) return endGame(r);
  const shooter = r.order[r.idx % n], round = Math.floor(r.idx / n) + 1, p = players.get(shooter);
  const upcoming = []; for (let k = 1; k <= 3 && r.idx + k < total; k++) upcoming.push((players.get(r.order[(r.idx + k) % n]) || {}).name || '?');
  r.turn = { phase: 'intro', shooter, name: p ? p.name : '?', round, shotNo: r.results[shooter].length + 1, of: CFG.shotsPerPlayer, idx: r.idx, total,
    seed: crypto.randomInt(1, 2 ** 31 - 1), diff: r.diff, startedAt: Date.now(), until: Date.now() + CFG.introMs, upcoming };
  broadcastRoom(r);
  r.timer = setTimeout(() => {
    if (!p || !p.ws) { finishTurn(r, { score: 0, kind: 'offline' }); return; }
    r.turn.phase = 'live'; r.turn.until = Date.now() + CFG.liveMs; broadcastRoom(r);
    r.timer = setTimeout(() => finishTurn(r, { score: 0, kind: 'timeout' }), CFG.liveMs);
  }, CFG.introMs);
}
function finishTurn(r, res) {
  if (!r.turn || r.turn.phase === 'result') return;
  clearTimeout(r.timer);
  const shot = { score: clampInt(res.score, 0, CFG.maxShotScore), hit: !!res.hit, miss: res.miss == null ? null : clampInt(res.miss, 0, 9999), kind: cleanText(res.kind || 'shot', 12), range: res.range == null ? null : clampInt(res.range, 0, 20000) };
  r.results[r.turn.shooter].push(shot);
  r.turn.phase = 'result'; r.turn.result = shot; r.turn.until = Date.now() + CFG.resultMs;
  r.idx++;
  broadcastRoom(r);
  r.timer = setTimeout(() => nextTurn(r), CFG.resultMs);
}
function endGame(r) {
  clearTimeout(r.timer);
  r.phase = 'finished'; r.turn = null;
  const ranking = r.order.map(id => { const res = r.results[id] || [], p = players.get(id);
    return { id, name: p ? p.name : '?', total: res.reduce((a, s) => a + s.score, 0), hits: res.filter(s => s.hit).length, best: Math.max(0, ...res.map(s => s.score)), left: !r.members.includes(id) }; })
    .sort((a, b) => b.total - a.total || b.hits - a.hits || b.best - a.best);
  ranking.forEach((x, i) => { x.place = i + 1; });
  if (r.order.length >= 2) for (const x of ranking) { const p = players.get(x.id); if (p && !x.left) addScore(p, 'tour', x.total, x.hits, CFG.shotsPerPlayer, r.diff); }
  r.lastRanking = ranking;
  broadcastRoom(r);
  for (const id of r.members) send(players.get(id), 'over', { ranking, room: r.id });
  if (r.quick) r.timer = setTimeout(() => { for (const id of r.members) { const p = players.get(id); if (p) { p.room = null; send(p, 'left', { why: 'closed' }); } } rooms.delete(r.id); }, 60000);
}

/* ---------------- hızlı maç ---------------- */
function quickTick() {
  quickQueue = quickQueue.filter(id => { const p = players.get(id); return p && p.ws && !p.room; });
  for (const id of quickQueue) send(players.get(id), 'quick', { state: 'waiting', count: quickQueue.length });
  if (quickQueue.length >= CFG.quickMax) return makeQuick();
  if (quickQueue.length >= 2 && !quickTimer) quickTimer = setTimeout(() => { quickTimer = null; if (quickQueue.length >= 2) makeQuick(); }, CFG.quickWaitMs);
}
function makeQuick() {
  clearTimeout(quickTimer); quickTimer = null;
  const ids = quickQueue.splice(0, CFG.quickMax);
  const r = newRoom({ name: 'Hızlı Maç', hostId: null, max: ids.length, diff: 'NORMAL', quick: true });
  for (const id of ids) { const p = players.get(id); r.members.push(id); p.room = r.id; send(p, 'quick', { state: 'matched' }); }
  broadcastRoom(r); roomSys(r, 'quickstart', { n: ids.length });
  r.timer = setTimeout(() => startGame(r), 3000);
  if (quickQueue.length) quickTick();
}

/* ---------------- mesaj işleme ---------------- */
function handle(p, m) {
  const r = p.room ? rooms.get(p.room) : null;
  switch (m.t) {
    case 'name': { const n = cleanName(m.name); if (!n) return send(p, 'err', { code: 'name' }); p.name = n; send(p, 'welcome', { id: p.pid, name: p.name, online: onlineCount(), ver: VERSION }); if (r) broadcastRoom(r); break; }
    case 'lb': send(p, 'lb', leaderboard(['day', 'week', 'month'].includes(m.period) ? m.period : 'day', m.kind === 'tour' ? 'tour' : 'solo', p.pid)); break;
    case 'submit': {
      const key = cleanText(m.key, 40); if (key && p.submitted.has(key)) return; if (key) p.submitted.add(key);
      addScore(p, 'solo', clampInt(m.score, 0, CFG.maxSoloScore), clampInt(m.hits, 0, 9), clampInt(m.shots, 0, 9), DIFFS.includes(m.diff) ? m.diff : 'NORMAL');
      send(p, 'submitted', { key }); break;
    }
    case 'rooms': send(p, 'rooms', { list: openRooms(), online: onlineCount() }); break;
    case 'create': {
      if (p.room) removeFromRoom(p, 'left');
      quickQueue = quickQueue.filter(id => id !== p.pid);
      const name = cleanText(m.name, 24) || `${p.name} Turnuvası`;
      const room = newRoom({ name, hostId: p.pid, max: clampInt(m.max, 2, CFG.roomMaxLimit), diff: DIFFS.includes(m.diff) ? m.diff : 'NORMAL' });
      room.members.push(p.pid); p.room = room.id; broadcastRoom(room); roomSys(room, 'created', { name: p.name }); break;
    }
    case 'join': {
      const room = m.code ? [...rooms.values()].find(x => x.code === String(m.code).toUpperCase().trim()) : rooms.get(m.roomId);
      if (!room || room.quick) return send(p, 'err', { code: 'noroom' });
      if (room.members.includes(p.pid)) { p.room = room.id; return broadcastRoom(room); }
      if (room.phase !== 'lobby') return send(p, 'err', { code: 'started' });
      if (room.members.length >= room.max) return send(p, 'err', { code: 'full' });
      if (p.room && p.room !== room.id) removeFromRoom(p, 'left');
      quickQueue = quickQueue.filter(id => id !== p.pid);
      if (!room.pending.includes(p.pid)) room.pending.push(p.pid);
      p.room = room.id; p.pendingFor = room.id;
      send(p, 'pending', { name: room.name, code: room.code, host: (players.get(room.hostId) || {}).name });
      send(players.get(room.hostId), 'room', { room: roomPublic(room, room.hostId) });
      send(players.get(room.hostId), 'request', { id: p.pid, name: p.name });
      break;
    }
    case 'approve': case 'reject': {
      if (!r || r.hostId !== p.pid) return send(p, 'err', { code: 'nothost' });
      const q = players.get(m.id); if (!r.pending.includes(m.id)) return;
      r.pending = r.pending.filter(id => id !== m.id);
      if (m.t === 'approve' && q && r.phase === 'lobby' && r.members.length < r.max) {
        r.members.push(m.id); q.room = r.id; q.pendingFor = null; roomSys(r, 'joined', { name: q.name });
      } else if (q) { q.room = null; q.pendingFor = null; send(q, 'rejected', { name: r.name, why: m.t === 'reject' ? 'host' : 'full' }); }
      broadcastRoom(r); break;
    }
    case 'kick': { if (!r || r.hostId !== p.pid || m.id === p.pid || r.phase === 'playing') return; const q = players.get(m.id); if (q && r.members.includes(m.id)) { removeFromRoom(q, 'kick'); send(q, 'left', { why: 'kick' }); } break; }
    case 'leave': {
      if (r && r.pending.includes(p.pid)) { r.pending = r.pending.filter(id => id !== p.pid); p.room = null; send(players.get(r.hostId), 'room', { room: roomPublic(r, r.hostId) }); }
      else if (r) removeFromRoom(p, 'left');
      quickQueue = quickQueue.filter(id => id !== p.pid); send(p, 'left', { why: 'self' }); break;
    }
    case 'start': {
      if (!r || r.hostId !== p.pid) return send(p, 'err', { code: 'nothost' });
      if (r.phase === 'playing') return;
      if (r.members.length < 2) return send(p, 'err', { code: 'need2' });
      startGame(r); break;
    }
    case 'again': { if (!r || r.phase !== 'finished' || (r.hostId && r.hostId !== p.pid)) return; r.phase = 'lobby'; r.order = []; r.results = {}; r.turn = null; broadcastRoom(r); break; }
    case 'quick': {
      if (p.room) removeFromRoom(p, 'left');
      if (!quickQueue.includes(p.pid)) quickQueue.push(p.pid); quickTick(); break;
    }
    case 'quickCancel': quickQueue = quickQueue.filter(id => id !== p.pid); send(p, 'quick', { state: 'cancelled' }); break;
    case 'chat': {
      if (!r || !r.members.includes(p.pid)) return;
      const now = Date.now(); if (now - p.lastChat < 600) return; p.lastChat = now;
      const text = cleanText(m.text, 200); if (!text) return;
      const msg = { from: p.pid, name: p.name, text, ts: now }; r.chat.push(msg); if (r.chat.length > 60) r.chat.shift();
      for (const id of r.members) send(players.get(id), 'chat', msg); break;
    }
    case 'snap': {
      if (!r || !r.turn || r.turn.phase !== 'live' || r.turn.shooter !== p.pid) return;
      relay(r, p.pid, JSON.stringify({ t: 'snap', d: m.d, g: r.game, i: r.turn.idx })); break;
    }
    case 'result': {
      if (!r || !r.turn || r.turn.phase !== 'live' || r.turn.shooter !== p.pid || m.i !== r.turn.idx) return;
      finishTurn(r, m); break;
    }
    case 'sync': if (r) send(p, 'room', { room: roomPublic(r, p.pid), chat: r.chat.slice(-30) }); break;
    case 'ping': send(p, 'pong', { ts: m.ts, online: onlineCount() }); break;
  }
}

/* ---------------- HTTP + WebSocket ---------------- */
const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (url === '/health' || (url === '/' && !fs.existsSync(path.join(PUBLIC_DIR, 'index.html')))) {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
    return res.end(JSON.stringify({ ok: true, name: 'stinger-server', ver: VERSION, online: onlineCount(), rooms: rooms.size }));
  }
  const file = path.join(PUBLIC_DIR, url === '/' ? 'index.html' : path.normalize(url).replace(/^(\.\.[\/\\])+/, ''));
  if (!file.startsWith(PUBLIC_DIR)) { res.writeHead(403); return res.end(); }
  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404); return res.end('not found'); }
    const ext = path.extname(file); res.writeHead(200, { 'Content-Type': ext === '.html' ? 'text/html; charset=utf-8' : ext === '.js' ? 'text/javascript' : 'application/octet-stream' }); res.end(buf);
  });
});
const wss = new WebSocketServer({ server, maxPayload: 64 * 1024 });
wss.on('connection', ws => {
  let p = null, msgCount = 0, windowStart = Date.now();
  ws.isAlive = true; ws.on('pong', () => { ws.isAlive = true; });
  ws.on('message', raw => {
    const now = Date.now(); if (now - windowStart > 1000) { windowStart = now; msgCount = 0; } if (++msgCount > 60) return;
    let m; try { m = JSON.parse(raw); } catch (e) { return; } if (!m || typeof m.t !== 'string') return;
    if (!p) {
      if (m.t !== 'hello') return;
      const dev = cleanText(m.deviceId, 64) || uid(16);
      let pid = byDevice.get(dev); p = pid && players.get(pid);
      if (p && p.ws && p.ws !== ws) { try { p.ws.close(4000, 'replaced'); } catch (e) {} }
      if (!p) { pid = crypto.createHash('sha256').update('stinger:' + dev).digest('base64url').slice(0, 12); /* yeniden başlatmada da aynı kimlik */ p = { pid, deviceId: dev, name: 'Atıcı', ws: null, room: null, lastChat: 0, submitted: new Set(), offlineAt: 0 }; players.set(pid, p); byDevice.set(dev, pid); }
      p.ws = ws; p.offlineAt = 0; p.name = cleanName(m.name) || p.name;
      send(p, 'welcome', { id: p.pid, name: p.name, online: onlineCount(), ver: VERSION });
      const r = p.room && rooms.get(p.room); if (r) { if (r.members.includes(p.pid)) { send(p, 'room', { room: roomPublic(r, p.pid), chat: r.chat.slice(-30) }); roomSys(r, 'back', { name: p.name }); } else if (r.pending.includes(p.pid)) send(p, 'pending', { name: r.name, code: r.code }); }
      return;
    }
    try { handle(p, m); } catch (e) { console.error('işleme hatası', e); }
  });
  ws.on('close', () => {
    if (!p || p.ws !== ws) return;
    p.ws = null; p.offlineAt = Date.now();
    quickQueue = quickQueue.filter(id => id !== p.pid);
    const r = p.room && rooms.get(p.room);
    if (r) { if (r.phase === 'playing' && r.turn && r.turn.shooter === p.pid && r.turn.phase === 'live') finishTurn(r, { score: 0, kind: 'offline' }); broadcastRoom(r); }
  });
});
// ölü bağlantıları ve uzun süre çevrimdışı kalanları temizle
setInterval(() => {
  for (const ws of wss.clients) { if (!ws.isAlive) { ws.terminate(); continue; } ws.isAlive = false; try { ws.ping(); } catch (e) {} }
  const now = Date.now();
  for (const p of players.values()) {
    if (p.ws || !p.offlineAt || now - p.offlineAt < CFG.offlineGraceMs) continue;
    if (p.room) { const r = rooms.get(p.room); if (r && r.phase !== 'playing') removeFromRoom(p, 'left'); else if (!r) p.room = null; }
    if (!p.room && now - p.offlineAt > 6 * 3600e3) { players.delete(p.pid); byDevice.delete(p.deviceId); }
  }
}, 15000);

storageReady.finally(() => server.listen(PORT, () => console.log(`STINGER sunucusu ${VERSION} · port ${PORT} · kayıt: ${useUpstash ? 'Upstash Redis' : DATA_DIR}`)));
module.exports = { server, periodKey, leaderboard, CFG, storageReady };
