// Routing tests for the atdeadlock voice relay. Run: npm test
'use strict';

const assert = require('assert');
const WebSocket = require('ws');
process.env.QUIET = '1';
process.env.WHITELIST_FILE = require('path').join(require('os').tmpdir(), `wl-test-${process.pid}.json`);
const { createServer, rooms } = require('./server');

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

/** opts: { room, client, build } — defaults are a current atdeadlock client. */
function connect(url, playerName, opts = {}) {
  const room = opts.room ?? 'ATDEADLOCK';
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(url);
    const c = { ws, name: playerName, id: 0, audio: [], ends: [], members: [], errors: [], video: [], viewers: null, features: [] };
    ws.on('open', () => {
      const hello = { type: 'hello', v: 1, partyCode: room, playerName, password: '' };
      if (opts.client !== null) hello.client = opts.client ?? 'atdeadlock';
      if (opts.build !== null) hello.clientBuild = opts.build ?? 2;
      ws.send(JSON.stringify(hello));
    });
    ws.on('message', (data, isBinary) => {
      if (isBinary) {
        if (data[0] === 0x01) c.audio.push({ from: data.readUInt16BE(1), seq: data.readUInt32BE(3), opus: data.subarray(7) });
        if (data[0] === 0x02) c.ends.push(data.readUInt16BE(1));
        if (data[0] === 0x03) c.video.push({ from: data.readUInt16BE(1), seq: data.readUInt32BE(3), jpeg: data.subarray(7) });
        return;
      }
      const msg = JSON.parse(data.toString());
      if (msg.type === 'welcome') { c.id = msg.id; c.members = msg.members; c.features = msg.features || []; c.admin = msg.whitelistAdmin; resolve(c); }
      if (msg.type === 'members') c.members = msg.members;
      if (msg.type === 'viewers') c.viewers = msg.names;
      if (msg.type === 'watch_denied') c.denied = (c.denied || 0) + 1;
      if (msg.type === 'whitelist_denied') c.wlDenied = (c.wlDenied || 0) + 1;
      if (msg.type === 'error') { c.errors.push(msg.code); resolve(c); }
    });
    ws.on('error', reject);
  });
}

function audio(seq, bytes) {
  const b = Buffer.alloc(5 + bytes.length);
  b[0] = 0x01; b.writeUInt32BE(seq, 1); Buffer.from(bytes).copy(b, 5);
  return b;
}

function frame(seq, size) {
  const b = Buffer.alloc(5 + size, 0xAB);
  b[0] = 0x03; b.writeUInt32BE(seq, 1);
  return b;
}

(async () => {
  const { server, wss } = createServer();
  await new Promise(r => server.listen(0, r));
  const url = `ws://127.0.0.1:${server.address().port}`;
  let passed = 0;
  const ok = (name) => { passed++; console.log('  ok -', name); };

  // ---- only atdeadlock, only current builds
  const zydel = await connect(url, 'Zed', { room: 'GLOBAL', client: null, build: null });
  assert.deepStrictEqual(zydel.errors, ['WRONG_CLIENT']); ok('ZydelNet (GLOBAL room, no client tag) is refused');
  const party = await connect(url, 'Pat', { room: 'STD' });
  assert.deepStrictEqual(party.errors, ['WRONG_CLIENT']); ok('other rooms are refused');
  const other = await connect(url, 'Ozzy', { client: 'zydelnet' });
  assert.deepStrictEqual(other.errors, ['WRONG_CLIENT']); ok('other mods are refused');
  const old = await connect(url, 'Olly', { build: null });
  assert.deepStrictEqual(old.errors, ['OUTDATED']); ok('old atdeadlock (no build number) is refused');
  const old1 = await connect(url, 'Olly', { build: 1 });
  assert.deepStrictEqual(old1.errors, ['OUTDATED']); ok('atdeadlock build 1 is refused');
  const newer = await connect(url, 'Nova', { build: 3 });
  assert.strictEqual(newer.errors.length, 0); ok('newer builds are accepted');
  newer.ws.close();
  for (const c of [zydel, party, other, old, old1]) c.ws.close();
  await sleep(100);

  // ---- voice routing
  const alice = await connect(url, 'Alice');
  const bob = await connect(url, 'Bob');
  await sleep(100);
  assert.strictEqual(alice.members.length, 2); ok('members see each other');
  alice.ws.send(audio(1, [1, 2, 3]));
  alice.ws.send(audio(2, [4, 5]));
  alice.ws.send(Buffer.from([0x02]));
  await sleep(150);
  assert.strictEqual(bob.audio.length, 2);
  assert.strictEqual(bob.audio[0].from, alice.id);
  assert.deepStrictEqual([...bob.audio[0].opus], [1, 2, 3]); ok('audio forwarded');
  assert.deepStrictEqual(bob.ends, [alice.id]); ok('end-of-transmission forwarded');
  assert.strictEqual(alice.audio.length, 0); ok('sender does not hear itself');

  alice.ws.send(JSON.stringify({ type: 'state', muted: true, disabled: false }));
  await sleep(50);
  alice.ws.send(audio(3, [9]));
  await sleep(100);
  assert.strictEqual(bob.audio.length, 2); ok('muted sender is not forwarded');
  alice.ws.send(JSON.stringify({ type: 'state', muted: false, disabled: false }));
  await sleep(50);

  for (let i = 0; i < 400; i++) alice.ws.send(audio(100 + i, [7]));
  await sleep(300);
  assert.ok(bob.audio.length - 2 <= 130, `rate limit let through ${bob.audio.length - 2}`); ok('flood is rate limited');

  const bob2 = await connect(url, 'bob');
  await sleep(150);
  assert.strictEqual(alice.members.length, 2); ok('reconnect replaces stale session');
  bob2.ws.close();
  await sleep(150);
  assert.strictEqual(alice.members.length, 1); ok('leaving updates members');
  alice.ws.close();
  await sleep(150);
  assert.ok(!rooms.has('ATDEADLOCK')); ok('empty room is removed');

  // ---- whitelist admins + listener routing
  const admin = await connect(url, 'FrankyOak');
  const sh = await connect(url, 'Shay');
  const vw = await connect(url, 'Vic');
  const by = await connect(url, 'Bea');
  await sleep(100);
  assert.strictEqual(admin.admin, true);
  assert.strictEqual(sh.admin, false); ok('only FrankyOak is whitelist admin');
  sh.ws.send(JSON.stringify({ type: 'whitelist_add', name: 'Shay' }));
  await sleep(100);
  assert.strictEqual(sh.wlDenied, 1); ok('non-admin cannot edit the whitelist');

  // ---- game view sharing
  assert.deepStrictEqual(sh.features, ['screen']); ok('welcome advertises screen feature');
  sh.ws.send(frame(1, 1000));
  await sleep(100);
  assert.strictEqual(vw.video.length + by.video.length, 0); ok('frames dropped while not sharing');
  sh.ws.send(JSON.stringify({ type: 'share', on: true }));
  await sleep(100);
  assert.ok(by.members.find(m => m.name === 'Shay').sharing); ok('sharing flag shown');
  by.ws.send(JSON.stringify({ type: 'watch', id: sh.id }));
  await sleep(100);
  assert.strictEqual(by.denied, 1); ok('non-listener cannot watch');
  admin.ws.send(JSON.stringify({ type: 'whitelist_add', name: 'Vic' }));
  await sleep(100);
  vw.ws.send(JSON.stringify({ type: 'watch', id: sh.id }));
  await sleep(100);
  assert.deepStrictEqual(sh.viewers, ['Vic']); ok('listener can watch; sharer told who');
  sh.ws.send(frame(2, 60 * 1024));
  await sleep(200);
  assert.strictEqual(vw.video.length, 1);
  assert.strictEqual(vw.video[0].jpeg.length, 60 * 1024); ok('viewer receives a 60 KB frame');
  assert.strictEqual(by.video.length + admin.video.length, 0); ok('non-viewers receive no frames');
  sh.ws.send(audio(1, [1]));
  await sleep(100);
  assert.strictEqual(vw.audio.length, 1);
  assert.strictEqual(by.audio.length, 0); ok('listener whitelist routes voice');
  for (let i = 0; i < 100; i++) sh.ws.send(frame(10 + i, 100));
  await sleep(300);
  assert.ok(vw.video.length - 1 <= 35, `video rate limit let through ${vw.video.length - 1}`); ok('frame flood is rate limited');
  admin.ws.send(JSON.stringify({ type: 'whitelist_remove', name: 'Vic' }));
  await sleep(100);
  assert.deepStrictEqual(sh.viewers, []);
  assert.ok(vw.denied >= 1); ok('removal from whitelist stops watching');
  sh.ws.send(JSON.stringify({ type: 'share', on: false }));
  await sleep(100);
  assert.ok(!by.members.find(m => m.name === 'Shay').sharing); ok('stop sharing clears flag');
  for (const c of [admin, sh, vw, by]) c.ws.close();

  wss.close(); server.close();
  try { require('fs').unlinkSync(process.env.WHITELIST_FILE); } catch (_) {}
  console.log(`\n${passed} checks passed`);
  process.exit(0);
})().catch(e => { console.error('FAILED:', e.message); process.exit(1); });
