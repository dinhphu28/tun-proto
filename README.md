# tun-proto

An open, language-agnostic wire specification for a **reverse TCP tunnel**: a
single authenticated transport connection multiplexes many logical TCP
connections using [HashiCorp yamux](https://github.com/hashicorp/yamux), with a
tiny per-stream header that names the target the server should dial.

This repository contains the **protocol specification only**, so that a compatible
**server** can be implemented in any runtime — Node.js, Java (Vert.x, Quarkus),
Scala (Akka HTTP), Go, Rust, etc. — and interoperate with an existing client.

## What's here

| Path | Purpose |
|------|---------|
| [`SPEC.md`](./SPEC.md) | The normative protocol specification (RFC 2119). |
| [`examples/`](./examples/) | Byte-exact framing examples: OpenRequest JSON + framed bytes, annotated yamux frames. |
| [`conformance/`](./conformance/) | Conformance checklist, the flow-control interop test, and a sample client config. |
| [`node/`](./node/) | Reference **Node.js server** implementation (`tun-proto-server`) — import and run, no yamux/WebSocket knowledge needed. |
| [`java/`](./java/) | **JVM libraries** on a shared sans-IO `yamux-core`: `vertx/` (embeddable Vert.x `TunnelServer`) and `quarkus/` (config-driven Quarkus library that auto-mounts). Non-blocking; verified against the Go client. |
| [`scala/`](./scala/) | **Scala 2.13 + Akka HTTP** library reusing `yamux-core`. `Future`/`ExecutionContext` on Java 21 virtual threads. Verified against the Go client. |

## Protocol at a glance

```
Local TCP conn ─► yamux stream ─► [transport] ─► yamux stream ─► Target TCP conn
   (client)        (mux client)     WS or HTTP     (mux server)      (server)
```

Three layers, bottom to top:

1. **Transport** — a full-duplex binary byte stream. Either a **WebSocket**
   (`GET /tunnels`, binary frames) or, for gateways that block WebSocket
   upgrades, an **HTTP dual-channel** fallback (a long-lived DOWN response +
   sequenced UP POSTs reassembled into one stream).
2. **Multiplexing** — HashiCorp **yamux** (version 0) over that byte stream. One
   transport connection carries many independent streams with per-stream flow
   control.
3. **Application framing** — the first bytes of every yamux stream are an
   **OpenRequest**: a 4-byte big-endian length prefix + a JSON object
   `{"network":"tcp","address":"host:port"}` telling the server what to dial.

## Implementing a compatible server

A conformant server acts as the **yamux server** (accepts streams, never
initiates them):

1. Accept the authenticated WebSocket upgrade at `GET /tunnels`.
2. Run yamux as the server over the binary stream.
3. For each accepted stream, read the OpenRequest, dial the named `tcp` target,
   and splice bytes transparently in both directions.

The single most important interop concern is **yamux flow control** (Window
Updates past the 256 KiB initial window). See the conformance checklist and the
bulk-transfer interop test in [`SPEC.md`](./SPEC.md#8-conformance-checklist).

> **Note on yamux libraries:** prefer a HashiCorp-compatible implementation.
> libp2p-flavored yamux shares the base framing but differs in window
> auto-tuning; validate with the interop test rather than trusting the label.

## Status

Specification version **1.0**. See [`SPEC.md`](./SPEC.md) for the authoritative
details and constants.

## License

Released under the [MIT License](./LICENSE).
