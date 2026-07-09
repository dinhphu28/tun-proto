# tun-proto-vertx

An **embeddable Vert.x library** that stands up a [tun-proto](../../SPEC.md)
reverse-TCP tunnel **server** in a few lines. Add the dependency, tell it how to
authenticate, and call `listen()` — you never touch yamux, WebSocket upgrades, or
the OpenRequest framing. It speaks the wire protocol the reference Go client
already uses.

```
Local app ─► tun-proto client ──WebSocket──► TunnelServer (this) ──► your TCP target
```

Built on the sans-IO `io.tunproto:yamux-core` engine. Fully non-blocking: one
`YamuxSession` per WebSocket, pinned to that connection's event loop.

## Dependency

```xml
<dependency>
  <groupId>io.tunproto</groupId>
  <artifactId>tun-proto-vertx</artifactId>
  <version>1.0.0</version>
</dependency>
```

Requires Java 21, Vert.x 4.5.x. `vertx-web` is an optional dependency, needed
only if you use `mount(Router)`.

## Quick start — own an HttpServer

```java
Vertx vertx = Vertx.vertx();

TunnelServer.create(vertx, new TunnelOptions()
        .addApiKey("my-secret-key"))                // accepted Bearer tokens
    .listen(8080)                                   // Future<TunnelServer>
    .onSuccess(s -> System.out.println("tunnel on ws://localhost:8080" + s.path()));
```

A client connecting to `ws://localhost:8080/tunnels` with
`Authorization: Bearer my-secret-key` can now forward local connections to any
TCP target this process can reach. That's the whole integration.

## Mount onto an existing app

Share your app's `HttpServer`/port — the tunnel only claims WebSocket upgrades on
its path (`/tunnels` by default); all your normal routes are untouched.

```java
// Via a vertx-web Router:
Router router = Router.router(vertx);
TunnelServer tunnel = TunnelServer.create(vertx, options);
tunnel.mount(router);
vertx.createHttpServer().requestHandler(router).listen(8080);

// Or directly onto an HttpServer (no vertx-web):
HttpServer server = vertx.createHttpServer();
tunnel.mount(server);
server.listen(8080);
```

`close()` closes an **owned** server (from `listen`) and the shared `NetClient`;
it never closes a **mounted** server.

## Authentication

Provide exactly one auth mode (validated at `create`):

```java
new TunnelOptions().setApiKeys(List.of("key1", "key2"));   // static allowlist (constant-time compare)

new TunnelOptions().setAuthenticator((bearerKey, ctx) ->   // custom, non-blocking
    myDb.isValidTunnelKey(bearerKey));                      // returns Future<Boolean>

new TunnelOptions().setAuthDisabled(true);                 // dev only — accepts every upgrade
```

A missing/malformed `Authorization: Bearer <key>` header, or a rejected key,
results in **HTTP 401** and no upgrade. Exceeding `maxSessions` results in **503**.

## Restricting where clients can connect (important)

By default, after authentication a client may ask the server to dial **any**
`host:port` it can reach — an SSRF surface (a one-time warning is logged). In
production, pass an egress policy:

```java
new TunnelOptions()
    .addApiKey("my-secret-key")
    .setAllowTarget((host, port) -> host.equals("10.0.0.5") && port == 5432);
```

Denied targets get the stream reset (RST); nothing is dialed.

## Options

| Setter | Default | Notes |
|---|---|---|
| `setPath` | `/tunnels` | WebSocket upgrade path. |
| `addApiKey` / `setApiKeys` | empty | Static Bearer allowlist. |
| `setAuthenticator` | none | Custom async predicate. |
| `setAuthDisabled` | `false` | Dev only. |
| `setAllowTarget` | allow-all + warn | `(host,port)->boolean` egress guard. |
| `setMaxStreamWindow` | `16777216` | Per-stream yamux window; matches the Go client. |
| `setMaxSessions` | `0` (∞) | Over-limit upgrades → 503. |
| `setMaxStreams` | `0` (∞) | Global concurrent proxied streams; over-limit new streams → RST. |
| `setMaxConcurrentStreamsPerSession` | `0` (∞) | Forwarded to the yamux config. |
| `setDialTimeout` | `10s` | Target connect timeout. |
| `setKeepAliveInterval` | `30s` | Per-session ping cadence. |
| `setKeepAliveTimeout` | `40s` | Torn down if no pong within this. |
| `setIdleTimeout` | `0` (off) | Optional WS idle timeout. |

## Lifecycle & metrics hooks

```java
TunnelServer.create(vertx, options)
    .onSession(e -> log.info("session up: {}", e.sessionId()))
    .onSessionClose(e -> log.info("session down: {}", e.sessionId()))
    .onStream(e -> log.info("stream {} -> {}:{}", e.streamId(), e.targetHost(), e.targetPort()))
    .onStreamError(e -> log.warn("stream error to {}: {}", e.targetAddress(), e.cause()))
    .listen(8080);

int sessions = tunnel.activeSessions();  // live gauges for your metrics
int streams  = tunnel.activeStreams();
```

## Interop with the Go client

```bash
cat > /tmp/tun.json <<'EOF'
{"server_url":"ws://127.0.0.1:8080/tunnels","api_key":"my-secret-key",
 "forwards":[{"local":"127.0.0.1:15432","remote":"127.0.0.1:5432"}]}
EOF
./tunnel-client-linux-amd64 -config /tmp/tun.json
```

Small echoes, large downloads (the SYN window delta is honored end-to-end so a
`>256 KiB` download does not stall at 262144 bytes), and concurrent multiplexed
streams all work.

## How it wires yamux-core (for maintainers)

- **DOWN transport:** each outbound yamux frame is wrapped zero-copy into a Vert.x
  `Buffer`, written to the WebSocket, and released **after** the write completes.
- **UP transport:** all WebSocket binary bytes are fed to `session.receive`
  (message boundaries are not frame boundaries).
- **Per stream:** `OpenRequestReader` parses the target, a shared `NetClient`
  dials it asynchronously, then the stream is spliced to the socket. Inbound bytes
  buffered while dialing are copied off the session accumulator (a shared, mutating
  buffer) before being queued.
- **UP replenish (OOM guard):** `stream.consumed(n)` is called only **after** the
  target socket accepts the bytes, so a slow target throttles WINDOW_UPDATE
  emission.
- **DOWN backpressure:** the target socket is paused when `stream.writeQueueFull()`
  and resumed on the stream's drain handler.
- **Transport backpressure:** `session.pauseOutbound()` when the WebSocket write
  queue is full, `resumeOutbound()` on its drain handler.
- **Keepalive:** a per-session periodic timer calls `session.sendPing()` +
  `session.tick(now)`; the session is torn down on pong timeout or WS close.

> Note on half-close: Vert.x `NetSocket.end()` performs a full TCP close (no
> half-close), so a peer FIN does not close the target write side — the target
> stays open so its response is never discarded; teardown happens on target EOF,
> target close, or RST.
