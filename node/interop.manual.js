'use strict';
// Cross-language interop check: the REAL Go tunnel-client binary <-> this Node
// server, end to end.
//
//   [driver] -> Go client local forward -> WebSocket -> tun-proto-server -> target
//
// Verifies WebSocket transport, yamux (Go<->JS), the OpenRequest, dial+splice,
// and flow control on a multi-MiB download (the case that exposes yamux window
// bugs). Kept out of `test/` so `npm test` does not try to run it. Run with:
//
//   GO_TUNNEL_CLIENT=/path/to/tunnel-client-linux-amd64 node interop.manual.js
//
// Exits 0 if all checks pass, non-zero otherwise (CI-friendly).

const net = require('net');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');
const { createTunnelServer } = require('.');

const GO_CLIENT = process.env.GO_TUNNEL_CLIENT;
const API_KEY = 'interop-key';
const DOWNLOAD_SIZE = 4 * 1024 * 1024;

const log = (...a) => console.log(new Date().toISOString().slice(11, 23), ...a);
const listenOn0 = (srv) => new Promise((r) => srv.listen(0, '127.0.0.1', () => r(srv.address().port)));
const freePort = () =>
  new Promise((r) => {
    const s = net.createServer();
    s.listen(0, '127.0.0.1', () => { const p = s.address().port; s.close(() => r(p)); });
  });

if (!GO_CLIENT) {
  log('SKIP: set GO_TUNNEL_CLIENT to the Go tunnel-client binary to run this test.');
  process.exit(0);
}
if (!fs.existsSync(GO_CLIENT)) {
  log(`SKIP: GO_TUNNEL_CLIENT does not exist: ${GO_CLIENT}`);
  process.exit(0);
}

async function main() {
  const results = [];
  const cleanups = [];
  let goProc;

  const overall = setTimeout(() => { log('OVERALL TIMEOUT'); shutdown(2); }, 45000);
  function shutdown(code) {
    clearTimeout(overall);
    try { goProc && goProc.kill('SIGKILL'); } catch {}
    for (const c of cleanups) { try { c(); } catch {} }
    setTimeout(() => process.exit(code), 200);
  }

  // Targets: an echo server and a large-download server.
  const echo = net.createServer((s) => { s.on('error', () => {}); s.pipe(s); });
  cleanups.push(() => echo.close());
  const echoPort = await listenOn0(echo);

  const blob = Buffer.alloc(DOWNLOAD_SIZE);
  for (let i = 0; i < blob.length; i++) blob[i] = i & 0xff;
  const dl = net.createServer((s) => { s.on('error', () => {}); s.resume(); s.end(blob); });
  cleanups.push(() => dl.close());
  const dlPort = await listenOn0(dl);

  // The tunnel server, restricting egress to the two targets.
  const tunnel = createTunnelServer({
    apiKeys: [API_KEY],
    allowTarget: (host, port) => host === '127.0.0.1' && (port === echoPort || port === dlPort),
  });
  tunnel.on('error', (e) => log('server error:', e.message));
  await tunnel.listen(0, '127.0.0.1');
  cleanups.push(() => tunnel.close());
  const serverPort = tunnel.address().port;
  log(`server on ${serverPort}; echo target ${echoPort}; download target ${dlPort}`);

  // Local forward ports for the Go client, and its config file.
  const localEcho = await freePort();
  const localDl = await freePort();
  const cfgPath = path.join(os.tmpdir(), `tun-proto-interop-${serverPort}.json`);
  fs.writeFileSync(
    cfgPath,
    JSON.stringify({
      server_url: `ws://127.0.0.1:${serverPort}/tunnels`,
      api_key: API_KEY,
      forwards: [
        { local: `127.0.0.1:${localEcho}`, remote: `127.0.0.1:${echoPort}` },
        { local: `127.0.0.1:${localDl}`, remote: `127.0.0.1:${dlPort}` },
      ],
    })
  );
  cleanups.push(() => { try { fs.unlinkSync(cfgPath); } catch {} });

  // Spawn the real Go client.
  goProc = spawn(GO_CLIENT, ['-config', cfgPath], { stdio: ['ignore', 'pipe', 'pipe'] });
  goProc.stdout.on('data', (d) => process.stdout.write('  [go] ' + d));
  goProc.stderr.on('data', (d) => process.stdout.write('  [go] ' + d));

  // Wait until the tunnel is live via a real round-trip.
  await waitFor(async () => {
    try { return (await echoRoundTrip(localEcho, Buffer.from('ping'))).equals(Buffer.from('ping')); }
    catch { return false; }
  }, 20000, 500);
  log('tunnel is live');

  // A: echo.
  await check(results, 'echo round-trip', async () => {
    const p = Buffer.from('hello from go-client interop');
    return (await echoRoundTrip(localEcho, p)).equals(p);
  });

  // B: multi-MiB download (cross-language flow control past 256 KiB).
  await check(results, `download ${DOWNLOAD_SIZE} bytes`, async () => {
    const got = await download(localDl);
    return got.length === DOWNLOAD_SIZE && got.equals(blob);
  });

  // C: concurrent multiplexed streams.
  await check(results, '3 concurrent streams', async () => {
    const payloads = [0, 1, 2].map((i) => Buffer.from(`stream-${i}-`.repeat(2000)));
    const echoed = await Promise.all(payloads.map((p) => echoRoundTrip(localEcho, p)));
    return echoed.every((e, i) => e.equals(payloads[i]));
  });

  log('==== RESULTS ====');
  let allOk = true;
  for (const [name, ok, note] of results) {
    if (!ok) allOk = false;
    log(`${ok ? 'PASS' : 'FAIL'}  ${name}${note ? '  (' + note + ')' : ''}`);
  }
  log(allOk ? '==== ALL PASS ====' : '==== FAILURES ====');
  shutdown(allOk ? 0 : 1);
}

async function check(results, name, fn) {
  try {
    const ok = await fn();
    results.push([name, ok]);
    log(`${ok ? 'PASS' : 'FAIL'}  ${name}`);
  } catch (e) {
    results.push([name, false, e.message]);
    log(`FAIL  ${name}  (${e.message})`);
  }
}

function echoRoundTrip(port, payload) {
  return new Promise((resolve, reject) => {
    const c = net.connect(port, '127.0.0.1');
    const chunks = [];
    let got = 0;
    const to = setTimeout(() => { c.destroy(); reject(new Error('echo timeout')); }, 8000);
    c.on('connect', () => c.write(payload));
    c.on('data', (d) => {
      chunks.push(d); got += d.length;
      if (got >= payload.length) { clearTimeout(to); c.end(); resolve(Buffer.concat(chunks)); }
    });
    c.on('error', (e) => { clearTimeout(to); reject(e); });
  });
}

function download(port) {
  return new Promise((resolve, reject) => {
    const c = net.connect(port, '127.0.0.1');
    const chunks = [];
    const to = setTimeout(() => { c.destroy(); reject(new Error('download timeout')); }, 20000);
    c.on('data', (d) => chunks.push(d));
    c.on('end', () => { clearTimeout(to); resolve(Buffer.concat(chunks)); });
    c.on('error', (e) => { clearTimeout(to); reject(e); });
  });
}

async function waitFor(fn, timeoutMs, intervalMs) {
  const start = Date.now();
  for (;;) {
    if (await fn()) return;
    if (Date.now() - start > timeoutMs) throw new Error('waitFor timed out');
    await new Promise((r) => setTimeout(r, intervalMs));
  }
}

main().catch((e) => { console.error('FATAL', e); process.exit(3); });
