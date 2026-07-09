'use strict';

// End-to-end smoke test exercising the real path:
//   yamux client -> WebSocket -> TunnelServer -> yamux -> OpenRequest -> dial -> echo
//
// Uses yamux-js as the client (the same library the spec targets for
// interop) so this validates framing, the OpenRequest, and flow control
// without needing the Go binary. Run with: npm test
//
// Cross-language interop with the Go client is covered separately by
// ../../conformance/interop-test.md.

const test = require('node:test');
const assert = require('node:assert');
const net = require('net');
const { WebSocket, createWebSocketStream } = require('ws');
const { Client: YamuxClient } = require('yamux-js');

const { createTunnelServer } = require('..');

function frameOpenRequest(address, network = 'tcp') {
  const json = Buffer.from(JSON.stringify({ network, address }), 'utf8');
  const hdr = Buffer.alloc(4);
  hdr.writeUInt32BE(json.length, 0);
  return Buffer.concat([hdr, json]);
}

// A TCP echo server used as the tunnel target.
function startEchoServer() {
  return new Promise((resolve) => {
    const srv = net.createServer((sock) => sock.pipe(sock));
    srv.listen(0, '127.0.0.1', () => resolve(srv));
  });
}

// Connect a yamux client over a WebSocket to the tunnel. Returns the client and
// a teardown() that clears every handle (keepalive disabled so the test process
// can exit promptly).
function connectClient(port, apiKey = 'test-key') {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/tunnels`, {
    headers: { Authorization: `Bearer ${apiKey}` },
  });
  return new Promise((resolve, reject) => {
    ws.once('open', () => {
      const transport = createWebSocketStream(ws);
      const client = new YamuxClient({
        enableKeepAlive: false,
        maxStreamWindowSize: 16 * 1024 * 1024,
      });
      client.on('error', () => {});
      transport.on('error', () => {});
      transport.pipe(client).pipe(transport);
      const teardown = () => {
        try { client.close(); } catch {}
        try { transport.destroy(); } catch {}
        try { ws.terminate(); } catch {}
      };
      resolve({ client, ws, teardown });
    });
    ws.once('error', reject);
  });
}

// Send a payload through one tunnel stream and collect exactly payload.length echoed bytes.
function roundTrip(client, address, payload) {
  return new Promise((resolve, reject) => {
    const stream = client.open();
    const chunks = [];
    let got = 0;
    stream.on('data', (d) => {
      chunks.push(d);
      got += d.length;
      if (got >= payload.length) resolve(Buffer.concat(chunks));
    });
    stream.on('error', reject);
    stream.write(frameOpenRequest(address));
    stream.write(payload);
  });
}

test('tunnel proxies bytes to the target (small payload)', { timeout: 10000 }, async () => {
  const echo = await startEchoServer();
  const echoAddr = `127.0.0.1:${echo.address().port}`;
  const tunnel = await createTunnelServer({ apiKeys: ['test-key'] }).listen(0, '127.0.0.1');
  const { client, teardown } = await connectClient(tunnel.address().port);

  try {
    const payload = Buffer.from('hello tunnel');
    const echoed = await roundTrip(client, echoAddr, payload);
    assert.strictEqual(echoed.toString(), payload.toString());
  } finally {
    teardown();
    await tunnel.close();
    echo.close();
  }
});

test('tunnel handles a >1 MiB payload (flow control past 256 KiB)', { timeout: 10000 }, async () => {
  const echo = await startEchoServer();
  const echoAddr = `127.0.0.1:${echo.address().port}`;
  const tunnel = await createTunnelServer({ apiKeys: ['test-key'] }).listen(0, '127.0.0.1');
  const { client, teardown } = await connectClient(tunnel.address().port);

  try {
    const payload = Buffer.alloc(2 * 1024 * 1024);
    for (let i = 0; i < payload.length; i++) payload[i] = i & 0xff;
    const echoed = await roundTrip(client, echoAddr, payload);
    assert.strictEqual(echoed.length, payload.length);
    assert.ok(echoed.equals(payload), 'echoed bytes must match exactly');
  } finally {
    teardown();
    await tunnel.close();
    echo.close();
  }
});

test('bad api key is rejected', { timeout: 10000 }, async () => {
  const tunnel = await createTunnelServer({ apiKeys: ['right-key'] }).listen(0, '127.0.0.1');
  const port = tunnel.address().port;
  try {
    await assert.rejects(
      new Promise((resolve, reject) => {
        const ws = new WebSocket(`ws://127.0.0.1:${port}/tunnels`, {
          headers: { Authorization: 'Bearer wrong-key' },
        });
        ws.once('open', () => resolve());
        ws.once('error', reject);
      })
    );
  } finally {
    await tunnel.close();
  }
});

test('disallowed target is refused', { timeout: 10000 }, async () => {
  const echo = await startEchoServer();
  const echoAddr = `127.0.0.1:${echo.address().port}`;
  const tunnel = await createTunnelServer({
    authDisabled: true,
    allowTarget: () => false,
  }).listen(0, '127.0.0.1');
  const { client, teardown } = await connectClient(tunnel.address().port);

  try {
    const stream = client.open();
    stream.resume(); // flowing, so the server's FIN surfaces as 'end'
    const closed = new Promise((resolve) => {
      stream.on('end', resolve);
      stream.on('close', resolve);
      stream.on('error', resolve);
    });
    stream.write(frameOpenRequest(echoAddr));
    await closed; // server tears the stream down instead of dialing
  } finally {
    teardown();
    await tunnel.close();
    echo.close();
  }
});
