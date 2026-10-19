/* Aschenarena – Server für Render (oder lokal): liefert das Spiel aus dem Ordner public/ aus
   und leitet Online-Nachrichten zwischen den Spielern eines Raums weiter (WebSocket unter /ws).
   Ohne zusätzliche Pakete: nur Node.js. Start: node server.js   (Port aus PORT, sonst 3000)

   Protokoll (passt zu public/relay.js):
     Spieler -> Server  {t:'join', room}            Raum betreten
     Server -> Spieler  {t:'welcome', id}           angenommen
     Server -> Spieler  {t:'full'}                  Raum voll (mehr als MAX_PER_ROOM)
     Server -> andere   {t:'joined', id} / {t:'left', id}
     Spieler -> Server  {t:'msg', topic, data}      wird an alle anderen im Raum weitergegeben
     Server -> andere   {t:'msg', topic, data, from} */
'use strict';
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = +process.env.PORT || 3000;
const PUBLIC = path.resolve(process.env.PUBLIC_DIR || path.join(__dirname, 'public'));
const MAX_PER_ROOM = +process.env.MAX_PER_ROOM || 4;   /* 2 gegen 2: Host + 3 Mitspieler */
const MAX_FRAME = 256 * 1024;

const MIME = {
  '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8', '.txt': 'text/plain; charset=utf-8', '.svg': 'image/svg+xml',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.webp': 'image/webp', '.gif': 'image/gif', '.ico': 'image/x-icon',
  '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg', '.wav': 'audio/wav', '.glb': 'model/gltf-binary', '.gltf': 'model/gltf+json',
  '.fbx': 'application/octet-stream', '.bin': 'application/octet-stream', '.woff2': 'font/woff2', '.woff': 'font/woff'
};

/* ---------- Dateien ausliefern ---------- */
const server = http.createServer(function (req, res) {
  let p = '/';
  try { p = decodeURIComponent(new URL(req.url, 'http://x').pathname); } catch (e) { res.writeHead(400); res.end(); return; }
  if (p === '/health') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok ' + rooms.size + ' Räume'); return; }
  if (p.endsWith('/')) p += 'index.html';
  const file = path.resolve(PUBLIC, '.' + p);
  if (file !== PUBLIC && !file.startsWith(PUBLIC + path.sep)) { res.writeHead(403); res.end(); return; }
  fs.stat(file, function (err, st) {
    if (err || !st.isFile()) { res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }); res.end('Nicht gefunden'); return; }
    res.writeHead(200, { 'Content-Type': MIME[path.extname(file).toLowerCase()] || 'application/octet-stream', 'Content-Length': st.size, 'Cache-Control': 'no-cache' });
    if (req.method === 'HEAD') { res.end(); return; }
    fs.createReadStream(file).pipe(res);
  });
});

/* ---------- WebSocket (RFC 6455, nur Text) ---------- */
const rooms = new Map();   /* Raumname -> Set von Verbindungen */
let nextId = 1;

function frame(op, payload) {
  const n = payload.length;
  let head;
  if (n < 126) { head = Buffer.alloc(2); head[1] = n; }
  else if (n < 65536) { head = Buffer.alloc(4); head[1] = 126; head.writeUInt16BE(n, 2); }
  else { head = Buffer.alloc(10); head[1] = 127; head.writeUInt32BE(Math.floor(n / 4294967296), 2); head.writeUInt32BE(n >>> 0, 6); }
  head[0] = 0x80 | op;
  return Buffer.concat([head, payload]);
}
function send(c, obj) {
  if (c.closed) return;
  try { c.sock.write(frame(1, Buffer.from(JSON.stringify(obj)))); } catch (e) { drop(c); }
}
function drop(c) {
  if (c.closed) return;
  c.closed = true;
  try { c.sock.destroy(); } catch (e) {}
  if (c.room) {
    const set = rooms.get(c.room);
    if (set) {
      set.delete(c);
      set.forEach(function (o) { send(o, { t: 'left', id: c.id }); });
      if (!set.size) rooms.delete(c.room);
    }
  }
}
function onText(c, txt) {
  let m;
  try { m = JSON.parse(txt); } catch (e) { return; }
  if (!m || typeof m !== 'object') return;
  if (m.t === 'join' && !c.room && typeof m.room === 'string') {
    const name = m.room.slice(0, 64);
    let set = rooms.get(name);
    if (!set) { set = new Set(); rooms.set(name, set); }
    if (set.size >= MAX_PER_ROOM) { send(c, { t: 'full' }); setTimeout(function () { drop(c); }, 200); return; }
    c.room = name;
    set.forEach(function (o) { send(o, { t: 'joined', id: c.id }); });
    set.add(c);
    send(c, { t: 'welcome', id: c.id, n: set.size });
  } else if (m.t === 'msg' && c.room) {
    const set = rooms.get(c.room);
    if (!set) return;
    const out = frame(1, Buffer.from(JSON.stringify({ t: 'msg', topic: m.topic, data: m.data, from: c.id })));
    set.forEach(function (o) { if (o !== c && !o.closed) { try { o.sock.write(out); } catch (e) { drop(o); } } });
  }
}
function onData(c, chunk) {
  c.buf = c.buf.length ? Buffer.concat([c.buf, chunk]) : chunk;
  while (c.buf.length >= 2) {
    const b0 = c.buf[0], b1 = c.buf[1], fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
    let len = b1 & 0x7f, off = 2;
    if (len === 126) { if (c.buf.length < 4) return; len = c.buf.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (c.buf.length < 10) return; len = c.buf.readUInt32BE(2) * 4294967296 + c.buf.readUInt32BE(6); off = 10; }
    if (len > MAX_FRAME) { drop(c); return; }
    const mo = off; if (masked) off += 4;
    if (c.buf.length < off + len) return;
    let data = c.buf.subarray(off, off + len);
    if (masked) { const k = c.buf.subarray(mo, mo + 4), d = Buffer.alloc(len); for (let i = 0; i < len; i++) d[i] = data[i] ^ k[i & 3]; data = d; }
    c.buf = c.buf.subarray(off + len);
    c.alive = true;
    if (op === 8) { try { c.sock.write(frame(8, Buffer.alloc(0))); } catch (e) {} drop(c); return; }
    if (op === 9) { try { c.sock.write(frame(10, data)); } catch (e) {} continue; }
    if (op === 10) continue;
    if (op === 1 || op === 2) { c.parts = [data]; }
    else if (op === 0 && c.parts) { c.parts.push(data); }
    else continue;
    if (fin && c.parts) { const txt = Buffer.concat(c.parts).toString('utf8'); c.parts = null; if (txt.length <= MAX_FRAME) onText(c, txt); }
  }
}
server.on('upgrade', function (req, sock) {
  let p = '/';
  try { p = new URL(req.url, 'http://x').pathname; } catch (e) {}
  const key = req.headers['sec-websocket-key'];
  if (p !== '/ws' || !key || String(req.headers.upgrade || '').toLowerCase() !== 'websocket') { sock.destroy(); return; }
  const acc = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + acc + '\r\n\r\n');
  sock.setNoDelay(true);
  const c = { id: 'p' + (nextId++) + Math.random().toString(36).slice(2, 6), sock: sock, buf: Buffer.alloc(0), room: null, closed: false, alive: true, parts: null };
  sock.on('data', function (d) { try { onData(c, d); } catch (e) { drop(c); } });
  sock.on('close', function () { drop(c); });
  sock.on('error', function () { drop(c); });
});
/* Verbindungen am Leben halten (Render trennt sonst stille Verbindungen) und Tote entfernen */
setInterval(function () {
  rooms.forEach(function (set) {
    set.forEach(function (c) {
      if (!c.alive) { drop(c); return; }
      c.alive = false;
      try { c.sock.write(frame(9, Buffer.alloc(0))); } catch (e) { drop(c); }
    });
  });
}, 25000);

server.listen(PORT, function () { console.log('Aschenarena-Server läuft auf Port ' + PORT + ' (Spiel aus ' + PUBLIC + ', bis ' + MAX_PER_ROOM + ' Spieler je Raum)'); });
