# tun-proto-server (Node.js)

A drop-in **server** for the [tun-proto](../SPEC.md) reverse TCP tunnel. Import
it, tell it how to authenticate, and call `listen()`. It speaks the wire protocol
that the reference Go client already uses — you never touch yamux, WebSocket
upgrades, or the OpenRequest framing.

```
Local app ─► tun-proto client ──WebSocket──► tun-proto-server (this) ──► your TCP target
```

## Install

```bash
npm install tun-proto-server
```

Requires Node.js ≥ 18.

## Quick start

```js
const { createTunnelServer } = require('tun-proto-server');

const tunnel = createTunnelServer({
  apiKeys: ['my-secret-key'], // accepted Bearer tokens
});

tunnel.listen(8080).then(() => {
  console.log('tunnel listening on ws://localhost:8080/tunnels');
});
```

A client connecting to `ws://localhost:8080/tunnels` with
`Authorization: Bearer my-secret-key` can now forward local connections to any
TCP target this process can reach. That's the whole integration.

## Authentication

Provide exactly one of:

```js
createTunnelServer({ apiKeys: ['key1', 'key2'] });          // static allowlist
createTunnelServer({ authenticate: async (key, req) => {    // custom check
  return await db.isValidTunnelKey(key);
}});
createTunnelServer({ authDisabled: true });                 // dev only
```

## Restricting where clients can connect (important)

By default, after authentication a client may ask the server to dial **any**
`host:port` it can reach — a server-side request-forgery (SSRF) surface. In
production, pass `allowTarget` to constrain egress:

```js
createTunnelServer({
  apiKeys: ['my-secret-key'],
  allowTarget: (host, port) => host === '10.0.0.5' && port === 5432,
});
```

## Sharing a port with an existing app

Attach to an `http.Server` you already have (the tunnel only handles WebSocket
upgrades on its `path`; your normal routes are untouched):

```js
const http = require('http');
const app = http.createServer(myRequestHandler);
createTunnelServer({ apiKeys: ['k'], server: app });
app.listen(8080);
```

## Options

| Option | Type | Default | Notes |
|--------|------|---------|-------|
| `apiKeys` | `string[]` | — | Static Bearer-token allowlist. |
| `authenticate` | `(key, req) => boolean \| Promise` | — | Custom auth predicate. |
| `authDisabled` | `boolean` | `false` | Accept everything (dev only). |
| `path` | `string` | `/tunnels` | WebSocket upgrade path. |
| `server` | `http.Server` | — | Reuse an existing server; `close()` won't close it. |
| `allowTarget` | `(host, port, ctx) => boolean \| Promise` | — | Egress policy (recommended). |
| `maxStreamWindowSize` | `number` | `262144` | Per-stream window. **Do not raise** — see below. |
| `dialTimeout` | `number` | `10000` | Target connect timeout (ms). |
| `maxSessions` | `number` | `0` (∞) | Concurrent sessions; over-limit upgrades get `503`. |
| `maxStreams` | `number` | `0` (∞) | Concurrent proxied streams across all sessions. |
| `debug` | `boolean` | `false` | Log lifecycle to the console. |

## Events

`TunnelServer` is an `EventEmitter`:

| Event | Payload | When |
|-------|---------|------|
| `listening` | address | Server bound. |
| `session` / `session-close` | `{ remoteAddress }` | A client tunnel opened / closed. |
| `stream` / `stream-close` | `{ address, remoteAddress }` | A proxied connection started / ended. |
| `stream-error` | `{ error, address?, remoteAddress? }` | An open request was rejected or a dial failed. |
| `error` | `Error` | Server-level error. |

```js
tunnel.on('stream', ({ address }) => console.log('proxying to', address));
tunnel.on('stream-error', ({ error }) => console.warn(error.message));
```

## Throughput & the window limit

`maxStreamWindowSize` defaults to **256 KiB** and you should leave it there.

This library builds on [`yamux-js`](https://www.npmjs.com/package/yamux-js)
`0.2.1`, which has a flow-control bug: it discards the window delta carried on
SYN-flagged `WINDOW_UPDATE` frames, and only emits a fresh `WINDOW_UPDATE` once
half of `maxStreamWindowSize` has been consumed. With a large window those two
behaviours **deadlock** any transfer between 256 KiB and `maxWindow / 2`. Pinning
the window to the 256 KiB yamux initial makes the throttle fire on every window,
so transfers of any size flow correctly (verified up to multi-MiB in the test
suite).

The practical effect: in-flight unacknowledged data per stream is bounded to
256 KiB, so single-stream throughput is roughly `256 KiB / RTT`. Over the
low-latency WebSocket path this is fine; over a high-RTT link it caps bulk
throughput. When talking to the **Go** reference client (whose flow control is
correct), the download direction (server→client) can still grow because the Go
side replenishes the window promptly.

Raising `maxStreamWindowSize` above 256 KiB logs a warning and will reintroduce
the deadlock until `yamux-js` is fixed or replaced.

## Compatibility

`yamux-js` is interoperable with HashiCorp yamux, so this server works with the
Go `tunnel-client`. Validate an end-to-end deployment with the
[conformance interop test](../conformance/interop-test.md) — especially the
bulk-transfer case.

## Testing

```bash
npm test
```

Spins up the server and drives it with a real WebSocket + yamux client (small
payload, >1 MiB transfer, bad-key rejection, egress rejection).

## License

MIT — see [../LICENSE](../LICENSE).
