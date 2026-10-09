// ZydelNet voice relay.
//
// Routes Opus voice frames between players in the same ZydelNet party.
// A "room" is a party code. Only sockets that joined a room receive that room's audio,
// so players outside the party never get your voice.
//
// Protocol (version 1)
//   Text frames (JSON)
//     C->S {"type":"hello","v":1,"partyCode":"ATDEADLOCK","playerName":"Steve","password":"",
//           "client":"atdeadlock","clientBuild":2}
//     Only the atdeadlock mod (room ATDEADLOCK) at clientBuild >= MIN_CLIENT_BUILD is accepted.
//     Anything else gets error WRONG_CLIENT (other mods / rooms) or OUTDATED (old atdeadlock).
//     C->S {"type":"state","muted":false,"disabled":false}
//     S->C {"type":"welcome","id":7,"members":[{"id":7,"name":"Steve","muted":false,"disabled":false}]}
//     S->C {"type":"members","members":[...]}
//     S->C {"type":"error","code":"BAD_PASSWORD"|"BAD_HELLO"|"ROOM_FULL"|"PROTOCOL"|"REPLACED"}
//   Binary frames
//     C->S 0x01 | seq:u32 | opus...        one 20 ms Opus frame
//     C->S 0x02                            end of transmission
//     S->C 0x01 | id:u16 | seq:u32 | opus... forwarded to every other room member
//     S->C 0x02 | id:u16                   forwarded end of transmission
//
// Game view sharing (atdeadlock "Share my Minecraft"). Old clients never send these and ignore them.
//   welcome carries "features":["screen"] so clients know this relay supports it.
//   Members carry "sharing":true while they share.
//     C->S {"type":"share","on":true|false}      start / stop sharing your game view
//     C->S {"type":"watch","id":7}               watch member 7's game view (0 = stop watching)
//     S->C {"type":"viewers","names":["Bob"]}    sent to a sharer: who is watching right now
//     C->S 0x03 | seq:u32 | jpeg...              one game frame (only while sharing)
//     S->C 0x03 | id:u16 | seq:u32 | jpeg...     forwarded ONLY to members watching that sharer
// Only players on the room's listener whitelist may watch. Empty whitelist = nobody can watch.
// Being removed from the whitelist stops your watching immediately.

'use strict';

const fs = require('fs');
const path = require('path');
const http = require('http');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PROTOCOL_VERSION = 1;
// This relay serves only the atdeadlock mod, and only builds at or above this number.
// Raise it (or set MIN_CLIENT_BUILD on Render) to force everyone onto a newer jar.
const REQUIRED_CLIENT = 'atdeadlock';
const ALLOWED_ROOMS = new Set(['ATDEADLOCK']);
const MIN_CLIENT_BUILD = parseInt(process.env.MIN_CLIENT_BUILD || '2', 10);
const PORT = parseInt(process.env.PORT || '8080', 10);
const HOST = process.env.HOST || '0.0.0.0';
const MAX_ROOM_SIZE = parseInt(process.env.MAX_ROOM_SIZE || '250', 10);
// Everyone with ZydelNet joins this room. It has no password, so nobody can lock others out of it.
const GLOBAL_ROOM = 'GLOBAL';
const MAX_OPUS_BYTES = 1000;
const HELLO_TIMEOUT_MS = 10_000;
const HEARTBEAT_MS = 25_000;
// Per-sender token bucket: 50 frames/s is real time; allow bursts after a network hiccup.
const BUCKET_RATE_PER_SEC = 60;
const BUCKET_CAPACITY = 120;
// Stop queueing audio to a slow receiver instead of adding latency.
const MAX_BUFFERED_BYTES = 64 * 1024;
// Game view frames: JPEG, normally 20-80 KB.
const MAX_FRAME_BYTES = 256 * 1024;
// Only queue a frame to a viewer whose socket has drained, so video adapts to their bandwidth
// and never piles up in front of voice.
const MAX_VIDEO_BUFFERED_BYTES = 64 * 1024;
// Audio may sit behind at most one frame, so allow that much extra before dropping audio.
const MAX_AUDIO_BUFFERED_BYTES = MAX_BUFFERED_BYTES + MAX_FRAME_BYTES;
// Per-sharer frame budget: 15 fps sustained, short bursts allowed.
const VIDEO_RATE_PER_SEC = 15;
const VIDEO_CAPACITY = 30;
// 'frameAck': the relay answers every game frame with {"type":"frame_ack"}, so a sharer only ever has
// one frame on the way and can never clog its own upload (which is what makes voice break up).
const FEATURES = ['screen', 'frameAck'];

const CODE_RE = /^[A-Z0-9]{3,16}$/;
const NAME_RE = /^[A-Za-z0-9_]{3,16}$/;

// Open rooms: no password, and each has its own listener whitelist.
//   GLOBAL     = ZydelNet
//   ATDEADLOCK = atdeadlock
// Whitelist empty = everyone hears everyone.
// Not empty = whitelisted players ("listeners") hear everyone, and everyone hears the listeners;
// players who aren't whitelisted don't hear each other.
// GLOBAL: anyone can add or remove names. ATDEADLOCK: only whitelist admins can (see below).
// Saved to disk (best effort; clients also re-sync after a relay restart).
const OPEN_ROOMS = new Set([GLOBAL_ROOM, 'ATDEADLOCK']);
// Rooms whose whitelist only admins may change.
const ADMIN_ONLY_ROOMS = new Set(['ATDEADLOCK']);
// Admin names (comma separated, case-insensitive). Override with the WHITELIST_ADMINS env var.
const WHITELIST_ADMINS = new Set((process.env.WHITELIST_ADMINS || 'FrankyOak')
  .split(',').map(n => n.trim().toLowerCase()).filter(Boolean));
// Names alone can be faked by a modified client. Set WHITELIST_ADMIN_KEY on the relay and put the same
// value in "whitelistAdminKey" in config/atdeadlock/voicechat.json so only the real admin is trusted.
const WHITELIST_ADMIN_KEY = process.env.WHITELIST_ADMIN_KEY || '';

function isWhitelistAdmin(name, key) {
  if (!WHITELIST_ADMINS.has(String(name).toLowerCase())) return false;
  if (!WHITELIST_ADMIN_KEY) return true;
  const a = Buffer.from(String(key || ''), 'utf8');
  const b = Buffer.from(WHITELIST_ADMIN_KEY, 'utf8');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

/** May this client change its room's whitelist? */
function canEditWhitelist(client) {
  return !!client.room && (!ADMIN_ONLY_ROOMS.has(client.room.code) || client.admin);
}
const WHITELIST_FILE = process.env.WHITELIST_FILE || path.join(__dirname, 'whitelist.json');
/** @type {Map<string, Set<string>>} room code -> lower-case names */
const whitelists = new Map([...OPEN_ROOMS].map(r => [r, new Set()]));
try {
  const data = JSON.parse(fs.readFileSync(WHITELIST_FILE, 'utf8'));
  const byRoom = Array.isArray(data) ? { [GLOBAL_ROOM]: data } : data; // old files were a plain GLOBAL list
  for (const [room, names] of Object.entries(byRoom || {})) {
    if (!whitelists.has(room) || !Array.isArray(names)) continue;
    for (const n of names) {
      if (typeof n === 'string' && NAME_RE.test(n)) whitelists.get(room).add(n.toLowerCase());
    }
  }
} catch (_) { /* no file yet */ }

function saveWhitelist() {
  const out = {};
  for (const [room, set] of whitelists) out[room] = [...set].sort();
  try { fs.writeFileSync(WHITELIST_FILE, JSON.stringify(out)); } catch (_) {}
}

function whitelistOf(room) {
  return whitelists.get(room.code) || null;
}

function isListener(room, name) {
  const wl = whitelistOf(room);
  return !!wl && wl.has(String(name).toLowerCase());
}

/** Can `receiver` hear audio from `sender` in this room? */
function canHear(room, receiver, sender) {
  const wl = whitelistOf(room);
  if (!wl || wl.size === 0) return true;
  return wl.has(receiver.name.toLowerCase()) || wl.has(sender.name.toLowerCase());
}

/** @type {Map<string, {code:string, passwordHash:Buffer, members:Map<number, any>}>} */
const rooms = new Map();
let nextId = 1;

function log(...args) {
  if (process.env.QUIET !== '1') console.log(new Date().toISOString(), ...args);
}

function hashPassword(pw) {
  return crypto.createHash('sha256').update(String(pw || ''), 'utf8').digest();
}

function allocateId() {
  // u16 ids, never 0; skip ids that are still in use.
  for (let i = 0; i < 65535; i++) {
    const id = nextId;
    nextId = nextId >= 65535 ? 1 : nextId + 1;
    let used = false;
    for (const room of rooms.values()) {
      if (room.members.has(id)) { used = true; break; }
    }
    if (!used) return id;
  }
  return 0;
}

function memberList(room) {
  return [...room.members.values()].map(m => ({
    id: m.id, name: m.name, muted: m.muted, disabled: m.disabled, listener: isListener(room, m.name),
    sharing: !!m.sharing,
  }));
}

function sendJson(ws, obj) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(obj));
}

function broadcastMembers(room) {
  const msg = JSON.stringify({ type: 'members', members: memberList(room) });
  for (const m of room.members.values()) {
    if (m.ws.readyState === m.ws.OPEN) m.ws.send(msg);
  }
}

function broadcastWhitelist(room) {
  const msg = JSON.stringify({ type: 'whitelist', names: [...(whitelistOf(room) || [])].sort() });
  for (const m of room.members.values()) {
    if (m.ws.readyState === m.ws.OPEN) m.ws.send(msg);
  }
  broadcastMembers(room); // listener flags changed
}

/** Tell a sharer who is watching them. */
function sendViewers(room, sharerId) {
  const sharer = room.members.get(sharerId);
  if (!sharer) return;
  const names = [];
  for (const m of room.members.values()) {
    if (m !== sharer && m.watching === sharerId && isListener(room, m.name)) names.push(m.name);
  }
  sendJson(sharer.ws, { type: 'viewers', names: names.sort() });
}

/** After a whitelist change: anyone no longer whitelisted stops watching; sharers get fresh viewer lists. */
function dropUnlistedWatchers(room) {
  for (const m of room.members.values()) {
    if (m.watching && !isListener(room, m.name)) {
      m.watching = 0;
      sendJson(m.ws, { type: 'watch_denied' });
    }
  }
  for (const m of room.members.values()) {
    if (m.sharing) sendViewers(room, m.id);
  }
}

function leaveRoom(client) {
  const room = client.room;
  if (!room) return;
  client.room = null;
  if (room.members.get(client.id) === client) room.members.delete(client.id);
  const watched = client.watching;
  client.watching = 0;
  client.sharing = false;
  if (room.members.size === 0) {
    rooms.delete(room.code);
    log(`room ${room.code} closed`);
  } else {
    broadcastMembers(room);
    if (watched) sendViewers(room, watched);
  }
}

function rejectAndClose(ws, code) {
  sendJson(ws, { type: 'error', code });
  setTimeout(() => { try { ws.close(4000, code); } catch (_) {} }, 50);
}

function handleHello(client, msg) {
  if (client.room) return rejectAndClose(client.ws, 'PROTOCOL');
  const code = typeof msg.partyCode === 'string' ? msg.partyCode.toUpperCase().replace(/[^A-Z0-9]/g, '') : '';
  const name = typeof msg.playerName === 'string' ? msg.playerName.trim() : '';
  if (msg.v !== PROTOCOL_VERSION || !CODE_RE.test(code) || !NAME_RE.test(name)) {
    return rejectAndClose(client.ws, 'BAD_HELLO');
  }
  if (msg.client !== REQUIRED_CLIENT || !ALLOWED_ROOMS.has(code)) {
    return rejectAndClose(client.ws, 'WRONG_CLIENT');
  }
  if (!Number.isInteger(msg.clientBuild) || msg.clientBuild < MIN_CLIENT_BUILD) {
    return rejectAndClose(client.ws, 'OUTDATED');
  }
  const pwHash = OPEN_ROOMS.has(code)
    ? hashPassword('')
    : hashPassword(typeof msg.password === 'string' ? msg.password.slice(0, 240) : '');

  let room = rooms.get(code);
  if (!room) {
    // The first member of a room sets its password, like creating the party.
    room = { code, passwordHash: pwHash, members: new Map() };
    rooms.set(code, room);
    log(`room ${code} opened`);
  } else if (!crypto.timingSafeEqual(room.passwordHash, pwHash)) {
    return rejectAndClose(client.ws, 'BAD_PASSWORD');
  }

  // Same player reconnecting: drop the stale socket so they don't appear twice.
  for (const other of room.members.values()) {
    if (other.name.toLowerCase() === name.toLowerCase()) {
      leaveRoom(other);
      rejectAndClose(other.ws, 'REPLACED');
    }
  }
  if (room.members.size >= MAX_ROOM_SIZE) {
    return rejectAndClose(client.ws, 'ROOM_FULL');
  }

  const id = allocateId();
  if (id === 0) return rejectAndClose(client.ws, 'ROOM_FULL');
  client.id = id;
  client.name = name;
  client.room = room;
  client.admin = isWhitelistAdmin(name, msg.adminKey);
  room.members.set(id, client);
  clearTimeout(client.helloTimer);

  sendJson(client.ws, { type: 'welcome', v: PROTOCOL_VERSION, id, members: memberList(room),
    whitelist: [...(whitelistOf(room) || [])].sort(), features: FEATURES,
    whitelistAdmin: canEditWhitelist(client) });
  broadcastMembers(room);
  log(`${name} joined ${code} (${room.members.size} in room)`);
}

function takeToken(client) {
  const now = Date.now();
  const elapsed = (now - client.bucketAt) / 1000;
  client.bucketAt = now;
  client.tokens = Math.min(BUCKET_CAPACITY, client.tokens + elapsed * BUCKET_RATE_PER_SEC);
  if (client.tokens < 1) return false;
  client.tokens -= 1;
  return true;
}

function takeVideoToken(client) {
  const now = Date.now();
  const elapsed = (now - client.videoAt) / 1000;
  client.videoAt = now;
  client.videoTokens = Math.min(VIDEO_CAPACITY, client.videoTokens + elapsed * VIDEO_RATE_PER_SEC);
  if (client.videoTokens < 1) return false;
  client.videoTokens -= 1;
  return true;
}

/** Game frames go only to members who chose to watch this sharer. */
function forwardVideo(client, packet) {
  const room = client.room;
  if (!room) return;
  for (const m of room.members.values()) {
    if (m === client || m.watching !== client.id) continue;
    if (!isListener(room, m.name)) continue; // only whitelisted listeners may watch
    const ws = m.ws;
    if (ws.readyState !== ws.OPEN) continue;
    if (ws.bufferedAmount > MAX_VIDEO_BUFFERED_BYTES) continue; // viewer is behind: skip this frame
    ws.send(packet, { binary: true });
  }
}

function forward(client, packet) {
  const room = client.room;
  if (!room) return;
  for (const m of room.members.values()) {
    if (m === client) continue;
    if (!canHear(room, m, client)) continue;
    const ws = m.ws;
    if (ws.readyState !== ws.OPEN) continue;
    if (packet[0] === 0x01 && ws.bufferedAmount > MAX_AUDIO_BUFFERED_BYTES) continue;
    ws.send(packet, { binary: true });
  }
}

function handleBinary(client, data) {
  if (!client.room || data.length < 1) return;
  const kind = data[0];
  if (kind === 0x01) {
    if (data.length < 6 || data.length - 5 > MAX_OPUS_BYTES) return;
    if (client.muted || client.disabled) return;
    if (!takeToken(client)) return;
    const out = Buffer.allocUnsafe(data.length + 2);
    out[0] = 0x01;
    out.writeUInt16BE(client.id, 1);
    data.copy(out, 3, 1); // seq + opus
    forward(client, out);
  } else if (kind === 0x02) {
    const out = Buffer.allocUnsafe(3);
    out[0] = 0x02;
    out.writeUInt16BE(client.id, 1);
    forward(client, out);
  } else if (kind === 0x03) {
    sendJson(client.ws, { type: 'frame_ack' }); // always, even if the frame is dropped below
    if (!client.sharing) return;
    if (data.length < 6 || data.length - 5 > MAX_FRAME_BYTES) return;
    if (!takeVideoToken(client)) return;
    const out = Buffer.allocUnsafe(data.length + 2);
    out[0] = 0x03;
    out.writeUInt16BE(client.id, 1);
    data.copy(out, 3, 1); // seq + jpeg
    forwardVideo(client, out);
  }
}

function handleText(client, text) {
  let msg;
  try { msg = JSON.parse(text); } catch (_) { return rejectAndClose(client.ws, 'PROTOCOL'); }
  if (!msg || typeof msg.type !== 'string') return rejectAndClose(client.ws, 'PROTOCOL');
  switch (msg.type) {
    case 'hello':
      return handleHello(client, msg);
    case 'whitelist_add':
    case 'whitelist_remove':
    case 'whitelist_sync': {
      const whitelist = client.room ? whitelistOf(client.room) : null;
      if (!whitelist) return;
      if (!canEditWhitelist(client)) {
        sendJson(client.ws, { type: 'whitelist_denied' });
        return;
      }
      const names = msg.type === 'whitelist_sync' ? (Array.isArray(msg.names) ? msg.names : []) : [msg.name];
      let changed = false;
      for (const raw of names.slice(0, 500)) {
        if (typeof raw !== 'string' || !NAME_RE.test(raw.trim())) continue;
        const n = raw.trim().toLowerCase();
        if (msg.type === 'whitelist_remove') {
          changed = whitelist.delete(n) || changed;
        } else if (!whitelist.has(n)) {
          whitelist.add(n);
          changed = true;
        }
      }
      if (changed) {
        saveWhitelist();
        broadcastWhitelist(client.room);
        dropUnlistedWatchers(client.room);
        log(`whitelist ${client.room.code} ${msg.type} by ${client.name}: ${[...whitelist].join(', ') || '(empty)'}`);
      }
      return;
    }
    case 'share': {
      if (!client.room) return;
      const on = msg.on === true;
      if (on === client.sharing) return;
      client.sharing = on;
      broadcastMembers(client.room);
      if (on) sendViewers(client.room, client.id);
      log(`${client.name} ${on ? 'started' : 'stopped'} sharing in ${client.room.code}`);
      return;
    }
    case 'watch': {
      if (!client.room) return;
      let id = Number.isInteger(msg.id) ? msg.id : 0;
      if (id === client.id || (id !== 0 && !client.room.members.has(id))) id = 0;
      if (id !== 0 && !isListener(client.room, client.name)) {
        sendJson(client.ws, { type: 'watch_denied' });
        id = 0;
      }
      if (id === client.watching) return;
      const previous = client.watching;
      client.watching = id;
      if (previous) sendViewers(client.room, previous);
      if (id) sendViewers(client.room, id);
      return;
    }
    case 'state': {
      if (!client.room) return;
      const muted = msg.muted === true;
      const disabled = msg.disabled === true;
      if (muted !== client.muted || disabled !== client.disabled) {
        client.muted = muted;
        client.disabled = disabled;
        broadcastMembers(client.room);
      }
      return;
    }
    default:
      return; // ignore unknown types for forward compatibility
  }
}

function createServer() {
  const server = http.createServer((req, res) => {
    // Health check for Render / uptime pingers.
    let members = 0;
    for (const r of rooms.values()) members += r.members.size;
    res.writeHead(200, { 'content-type': 'text/plain' });
    res.end(`atdeadlock voice relay ok rooms=${rooms.size} members=${members} minBuild=${MIN_CLIENT_BUILD}\n`);
  });

  // Large enough for one game frame; audio and control messages stay tiny.
  const wss = new WebSocketServer({ server, maxPayload: MAX_FRAME_BYTES + 64, perMessageDeflate: false });

  wss.on('connection', (ws) => {
    const client = {
      ws, id: 0, name: '', room: null, muted: false, disabled: false,
      tokens: BUCKET_CAPACITY, bucketAt: Date.now(), alive: true, helloTimer: null,
      sharing: false, watching: 0, videoTokens: VIDEO_CAPACITY, videoAt: Date.now(),
    };
    client.helloTimer = setTimeout(() => {
      if (!client.room) rejectAndClose(ws, 'BAD_HELLO');
    }, HELLO_TIMEOUT_MS);

    ws.on('pong', () => { client.alive = true; });
    ws.on('message', (data, isBinary) => {
      if (isBinary) handleBinary(client, data);
      else handleText(client, data.toString('utf8'));
    });
    ws.on('close', () => {
      clearTimeout(client.helloTimer);
      if (client.room) log(`${client.name} left ${client.room.code}`);
      leaveRoom(client);
    });
    ws.on('error', () => {});
    ws._zydelClient = client;
  });

  const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
      const c = ws._zydelClient;
      if (!c) continue;
      if (!c.alive) { ws.terminate(); continue; }
      c.alive = false;
      try { ws.ping(); } catch (_) {}
    }
  }, HEARTBEAT_MS);
  wss.on('close', () => clearInterval(heartbeat));

  return { server, wss };
}

if (require.main === module) {
  const { server } = createServer();
  // Bind IPv4 explicitly: on some Windows setups a bare listen() ends up IPv6-only,
  // so ws://127.0.0.1 is refused. Render also expects 0.0.0.0.
  server.listen(PORT, HOST, () => log(`ZydelNet voice relay listening on ${HOST}:${PORT}`));
}

module.exports = { createServer, rooms };
