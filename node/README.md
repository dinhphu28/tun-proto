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
| `maxStreamWindowSize` | `number` | `16777216` | Per-stream yamux window (16 MiB). Matches the Go client. |
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

## Flow control & the bundled yamux-js patch

This library builds on [`yamux-js`](https://www.npmjs.com/package/yamux-js)
`0.2.1`, which has a flow-control bug: `handleStreamMessage` **discards the
window delta carried on SYN-flagged `WINDOW_UPDATE` frames**. A peer that
advertises a large initial receive window on SYN — which HashiCorp yamux and the
Go tunnel client do (~16 MiB) — therefore leaves this server's send window stuck
at the 256 KiB yamux initial. Because that peer won't re-advertise until it has
consumed ~half its window, any bulk send larger than 256 KiB **deadlocks**. This
was reproduced against the real Go client: a 4 MiB download stalled at exactly
262144 bytes.

The library applies a small pinned runtime patch
([`src/yamux-patch.js`](./src/yamux-patch.js)) that honors the SYN window delta —
the one line upstream is missing. With it, the full **16 MiB** window works and
transfers stream at line rate. The patch is scoped to `yamux-js@0.2.1` (pinned in
`package.json`); revisit it if that version changes or upstream fixes the bug.

## Compatibility

`yamux-js` (with the patch above) is interoperable with HashiCorp yamux, so this
server works with the Go `tunnel-client`. This is **verified**, not assumed — see
[Interop testing](#interop-testing). Also validate your own end-to-end deployment
with the [conformance interop test](../conformance/interop-test.md).

## Interop testing

`interop.manual.js` drives the **real Go `tunnel-client` binary** against this
server end-to-end (echo, a multi-MiB download, and concurrent multiplexed
streams). It is not part of `npm test` because it needs the Go binary; point it
at one and run it directly:

```bash
GO_TUNNEL_CLIENT=/path/to/tunnel-client-linux-amd64 node interop.manual.js
```

It exits non-zero on any failure, so it is CI-friendly wherever the binary is
available.

## Testing

```bash
npm test
```

Spins up the server and drives it with a real WebSocket + yamux client (small
payload, >1 MiB transfer, bad-key rejection, egress rejection).

## License

MIT — see [../LICENSE](../LICENSE).
