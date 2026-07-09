# tun-proto-scala

An embeddable **Scala 2.13 + Akka HTTP** library that stands up a
[tun-proto](../SPEC.md) reverse-TCP tunnel **server** in a few lines. It hides
yamux, the WebSocket upgrade, and the OpenRequest framing entirely, and reuses the
sans-IO `io.tunproto:yamux-core` Java engine unchanged-in-shape (one interop fix,
see below). Concurrency is `Future`/`ExecutionContext` on Java 21 **virtual
threads**; blocking `java.net.Socket` IO is used for the targets.

```
Local app ─► tun-proto client ──WebSocket──► tun-proto-scala (this) ──► your TCP target
```

## Quick start

```scala
import akka.actor.ActorSystem
import io.tunproto.scala.{TunnelServer, TunnelOptions}

implicit val system: ActorSystem = ActorSystem("app")

val server  = TunnelServer(TunnelOptions(apiKeys = Set("secret")))
val binding = server.listen("0.0.0.0", 8080)     // Future[Http.ServerBinding]
// serves ws://0.0.0.0:8080/tunnels
```

Or embed the route into an existing Akka HTTP app (shares your port):

```scala
val route = server.route   // akka.http.scaladsl.server.Route
```

A client connecting to `ws://host:8080/tunnels` with
`Authorization: Bearer secret` can now forward local connections to any TCP target
this process can reach.

## Build

The Scala library depends on `io.tunproto:yamux-core:1.0.0` via **Maven Local**, so
publish the Java core first, then build with sbt (Java 21 required):

```bash
# 1) Publish the reused Java core to Maven Local (REQUIRED first).
(cd ../java && mvn -q -o -pl yamux-core -am install)

# 2) Compile / test the Scala library.
sbt clean test

# 3) Run the embedded example server (binds ws://0.0.0.0:8080/tunnels, key "secret").
sbt "runMain io.tunproto.scala.example.Main"
```

`JAVA_HOME` must point at a **JDK 21** (for `-release:21` compilation and runtime
virtual threads). `Test`/`run` are forked so the forked JVM is a real Java 21.

## Authentication

Provide exactly one mode (validated in `TunnelOptions`):

```scala
TunnelOptions(apiKeys = Set("key1", "key2"))                              // static allowlist
TunnelOptions(authenticator = Some((key: String) => db.isValid(key)))    // custom, async
TunnelOptions(authDisabled = true)                                       // dev only
```

The Bearer token is validated **before** the WebSocket upgrade completes; an
invalid/missing token gets `401` and no yamux session is created. Static keys are
compared with `MessageDigest.isEqual` to blunt timing.

## Restricting egress (important)

By default, after auth a client may ask the server to dial **any** `host:port` —
an SSRF surface (the library logs a one-time warning). In production pass
`allowTarget`:

```scala
TunnelOptions(
  apiKeys = Set("secret"),
  allowTarget = Some((host, port) => host == "10.0.0.5" && port == 5432)
)
```

Denied targets are RST before any dial.

## Options

| Option | Type | Default | Notes |
|---|---|---|---|
| `apiKeys` | `Set[String]` | `Set.empty` | Static Bearer allowlist. |
| `authDisabled` | `Boolean` | `false` | Accept everything (dev only). |
| `authenticator` | `Option[String => Future[Boolean]]` | `None` | Custom async predicate. |
| `allowTarget` | `Option[(String, Int) => Boolean]` | `None` | Egress policy (recommended). |
| `path` | `String` | `"tunnels"` | WebSocket path (multi-segment ok). |
| `maxStreamWindow` | `Int` | `262144` (256 KiB) | Per-stream yamux window. **See below** — do not raise above 512 KiB. |
| `maxConcurrentStreamsPerSession` | `Int` | `0` (∞) | Per-session cap; over → RST. |
| `maxSessions` | `Int` | `0` (∞) | Concurrent sessions; over → `503`. |
| `maxStreams` | `Int` | `0` (∞) | Global proxied streams; over → RST. |
| `dialTimeout` | `FiniteDuration` | `10s` | Target connect timeout. |
| `keepAliveInterval` | `FiniteDuration` | `30s` | Ping cadence. |
| `keepAliveTimeout` | `FiniteDuration` | `40s` | Teardown if no pong. |
| `blockingEc` | `Option[ExecutionContext]` | virtual-thread-per-task | Target socket IO EC. |

## Flow control: why `maxStreamWindow` defaults to 256 KiB (and is capped at 512 KiB)

`yamux-core` replenishes a stream's **receive** window (the UP direction:
peer → target) only once it has consumed `maxStreamWindow / 2` bytes
(`YamuxStream.consumed`), but the **initial** receive window is fixed at 256 KiB
(`YamuxConstants.INITIAL_WINDOW`) regardless of `maxStreamWindow`. If
`maxStreamWindow / 2` exceeds 256 KiB, the peer exhausts its 256 KiB window before
the server ever emits a `WINDOW_UPDATE`, and **an UP transfer larger than 256 KiB
deadlocks**. This was reproduced against the real Go client: a 4 MiB upload to a
sink target with a 16 MiB window hung.

We therefore default `maxStreamWindow` to **256 KiB** (replenish threshold 128 KiB
≤ initial window) and `require` it stays ≤ 512 KiB. The **DOWN** direction
(target → peer) is governed by the send window plus the SYN delta, which
`yamux-core` applies correctly, so large downloads stream fine at any of these
settings.

## Interop lessons honored

- Forward the OpenRequest **leftover** to the target first (no dropped bytes);
  DATA that arrives while dialing is buffered and flushed in order after connect.
- Call `stream.consumed(n)` only **after** the target socket accepts n bytes
  (throttles WINDOW_UPDATE replenish to target drain speed — OOM guard).
- Honor `writeQueueFull()` / `drainHandler` (DOWN backpressure: the target reader
  parks on a single non-recycled promise, checked atomically with
  `writeQueueFull()` on the session thread — no lost-wakeup) and
  `pauseOutbound` / `resumeOutbound` (WS transport backpressure, hysteresis
  watermarks) so a slow peer or target cannot OOM the server.
- UP writes to the target are strictly serialized (a per-stream write chain built
  on the session thread) so bytes are never reordered.
- Outbound WS frames are emitted strictly FIFO with a single in-flight
  `Source.queue.offer` (Akka forbids overlapping offers).
- Peer FIN does **not** close the target (it keeps flowing until the target EOFs,
  then we FIN the peer); errors RST.

### Verified against the real Go client

`ws://…/tunnels` with the reference Go `tunnel-client` was exercised end-to-end
(256 KiB window): a small echo, a **4 MiB echo round-trip** (byte-exact), a
**4 MiB upload-only to a sink** (no deadlock — the Blocker-1 case), and **8
concurrent multiplexed 512 KiB streams**, repeatedly, with zero protocol errors or
reconnects.

## One required `yamux-core` interop fix

The core originally rejected any inbound SYN whose stream id was not strictly
greater than the highest id seen (`stream id not strictly increasing`). Real
HashiCorp-yamux clients (the Go tunnel client) assign stream ids from a monotonic
counter under lock but emit each SYN lazily on the stream's first write, from that
caller's goroutine — so under **concurrent** stream opens the SYN *frames* reach
the wire out of id order (e.g. `SYN(3)` before `SYN(1)`), even though the ids are
assigned monotonically. The strict check GOAWAY'd the whole session and broke the
SPEC's own N ≥ 8 concurrent-stream conformance. The fix (in
`YamuxSession.ensureAcceptedStream`) accepts any not-currently-tracked odd id and
only advances the high-water mark upward; ids are still odd-only and are never
reused within a session. Core tests remain green (24/24).

## Licensing (non-negotiable pins)

This library pins the **last Apache-2.0** Akka releases to avoid the BSL:

- `akka-http` **10.2.10** (Apache 2.0)
- `akka-stream` / `akka-actor` / `akka-actor-typed` **2.6.20** (Apache 2.0)

Do **not** upgrade past these or you inherit the BSL.

## Architecture (one paragraph)

Each WebSocket connection gets one `YamuxSession` and one **session
ExecutionContext** — a single virtual thread that serializes every call into the
session and its streams (the yamux single-threaded contract, enforced with zero
locks). Inbound WS bytes are folded to strict and fed to `session.receive` on the
session EC. Outbound frames are copied to a `ByteString` and emitted via a
back-pressured `Source.queue`. Each accepted stream is spliced to a blocking
`java.net.Socket` dialed on a separate virtual-thread **blocking EC**; all core
touches from the blocking side hop back onto the session EC via `Future`
submission. Every `ByteBuf` is copied at each core↔Akka / core↔blocking boundary
and released exactly once (no live `ByteBuf` ever crosses a thread).
