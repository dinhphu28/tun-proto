# Tunnel Protocol Specification

Version: 1.0
Status: Normative

This document specifies the wire protocol used by the `cexp-sdk-backend` tunnel
so that a compatible **server** can be implemented in any language/runtime
(Node.js, Java/Vert.x, Java/Quarkus, Scala/Akka HTTP, etc.) and interoperate
with the existing Go **client** (`cmd/tunnel-client`).

The key words **MUST**, **MUST NOT**, **SHOULD**, **SHOULD NOT**, and **MAY** are
used as in RFC 2119.

---

## 1. Overview

The tunnel is a **reverse TCP forwarder**. A single transport connection carries
many independent logical TCP connections, multiplexed with
[HashiCorp yamux](https://github.com/hashicorp/yamux). Over each yamux stream the
client sends a small **OpenRequest** header naming a target address; the server
dials that target and splices bytes in both directions.

```
Local TCP conn ─► yamux stream ─► [transport] ─► yamux stream ─► Target TCP conn
   (client)        (mux client)     WS or HTTP     (mux server)      (server)
```

### 1.1 Roles

| Role | Runs where | yamux role | Responsibility |
|------|-----------|-----------|----------------|
| **Client** | Where connections originate (e.g. a laptop, a CI runner) | yamux **client** (opens streams) | Listens on local ports; opens one yamux stream per accepted local connection; writes the OpenRequest. |
| **Server** | Next to the target services (e.g. inside the cluster) | yamux **server** (accepts streams) | Accepts streams; reads the OpenRequest; dials the real target; splices bytes. |

An alternative implementation described by this document is a **Server**. It
**MUST** act as the yamux *server* (accept streams; never initiate them).

### 1.2 Layered architecture

```
┌───────────────────────────────────────────────┐
│ 3. Application framing — OpenRequest (§5)     │  per yamux stream
├───────────────────────────────────────────────┤
│ 2. Multiplexing — yamux profile (§4)          │  per transport connection
├───────────────────────────────────────────────┤
│ 1a. Transport — WebSocket (§3)                │  OR
│ 1b. Transport — HTTP dual-channel (Appendix A)│
└───────────────────────────────────────────────┘
```

A conformant server **MUST** implement §3 (WebSocket), §4 (yamux), and §5
(OpenRequest). Appendix A (HTTP dual-channel) is **OPTIONAL** and only required
for deployment behind gateways that block WebSocket upgrades.

---

## 2. Authentication

Every transport connection is authenticated with a static API key presented as an
HTTP Bearer token on the initial request (the WebSocket upgrade, or the DOWN/UP
HTTP requests).

```
Authorization: Bearer <api-key>
```

- The server **MUST** validate the key before completing the WebSocket handshake
  (or before returning `200` on the DOWN request).
- On an invalid or missing key the server **MUST** reject with HTTP `401
  Unauthorized` and **MUST NOT** proceed to the yamux layer.
- A server **MAY** provide a configuration switch to disable authentication for
  local development (the reference server uses `TUNNEL_AUTH_DISABLED=true`). This
  **SHOULD NOT** be enabled in production.

The API-key store, hashing, and issuance are out of scope for this document; only
the Bearer presentation on the wire is normative.

---

## 3. Transport A — WebSocket (REQUIRED)

### 3.1 Endpoint

```
GET /tunnels
Upgrade: websocket
Authorization: Bearer <api-key>
```

- The server **MUST** accept a standard [RFC 6455](https://www.rfc-editor.org/rfc/rfc6455)
  WebSocket upgrade at this path.
- Non-browser clients typically send **no** `Origin` header; the server **MUST
  NOT** reject a request solely for a missing or foreign `Origin` (the reference
  server sets `InsecureSkipVerify`). Origin enforcement **MAY** be added if
  browser-origin clients are introduced.

### 3.2 Message framing

- All application bytes **MUST** be carried in WebSocket **binary** frames
  (opcode `0x2`). Text frames **MUST NOT** be used.
- The WebSocket message boundaries are **not significant**: the yamux byte stream
  **MUST** be treated as a continuous full-duplex stream. An implementation
  **MUST NOT** assume that one WebSocket message equals one yamux frame. A yamux
  frame **MAY** span multiple WebSocket messages, and one WebSocket message
  **MAY** contain multiple (or partial) yamux frames.
- Standard WebSocket ping/pong (control frames) **MAY** be used for transport
  liveness but are independent of yamux keepalive (§4.6). yamux keepalive is the
  authoritative liveness mechanism.

### 3.3 Read limits

The reference client copies in 32 KiB chunks (`copyBufSize`). A server **SHOULD
NOT** impose a WebSocket per-message read limit smaller than the yamux maximum
frame size it advertises via flow control, or large Data frames will be rejected.
A read limit of at least the negotiated max stream window (§4.5) is **RECOMMENDED**.

### 3.4 Lifecycle

1. Client dials `GET /tunnels` with the Bearer header.
2. Server authenticates (§2) and, if a session cap is configured, admits or
   rejects the session (§6).
3. On success the server upgrades and immediately begins acting as the yamux
   **server** over the resulting binary stream (§4).
4. When the WebSocket closes (either side), the yamux session and all its streams
   **MUST** be torn down.

---

## 4. Multiplexing — yamux profile

The multiplexing layer is HashiCorp yamux, version `0`. This section restates the
wire format normatively and pins the configuration values the reference client
uses. The canonical reference is
<https://github.com/hashicorp/yamux/blob/master/spec.md>; where this document and
the upstream spec disagree, the upstream spec governs the framing and this
document governs the **profile** (constants in §4.5, §7).

### 4.1 Frame header

Every yamux frame begins with a fixed **12-byte** header in **network byte order
(big-endian)**:

```
 0               1               2               3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|    Version    |     Type      |            Flags              |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          StreamID                             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Length / Delta                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

| Field | Offset | Size | Meaning |
|-------|--------|------|---------|
| Version | 0 | 1 byte | **MUST** be `0x00`. |
| Type | 1 | 1 byte | Frame type (§4.2). |
| Flags | 2 | 2 bytes | Bitmask (§4.3). |
| StreamID | 4 | 4 bytes | Stream identifier (§4.4). |
| Length | 8 | 4 bytes | Meaning depends on Type (§4.2). |

### 4.2 Types

| Type | Value | Length field means | Payload |
|------|-------|--------------------|---------|
| Data | `0x0` | Number of payload bytes that follow the header | that many bytes |
| Window Update | `0x1` | Window size **delta** (bytes to add to the peer's send window) | none |
| Ping | `0x2` | Opaque value, echoed in the ACK reply | none |
| Go Away | `0x3` | Error code (§4.7) | none |

Only **Data** payload bytes consume flow-control window (§4.5). Window Update,
Ping, and Go Away carry no payload after the 12-byte header.

### 4.3 Flags

| Flag | Value | Meaning |
|------|-------|---------|
| SYN | `0x1` | Start a new stream. Carried on the first Data/Window Update for a stream (or on a Ping). |
| ACK | `0x2` | Acknowledge a stream the peer opened. |
| FIN | `0x4` | Half-close: sender will send no more Data on this stream. |
| RST | `0x8` | Hard reset: close the stream immediately. |

Flags are a bitmask and **MAY** be combined (e.g. `SYN` on the first data frame).

### 4.4 Stream identifiers

- StreamID `0x0` is reserved for **session-level** frames (Ping, Go Away) and
  **MUST NOT** be used for stream data.
- The **client** (stream initiator) uses **odd** StreamIDs, increasing.
- The **server** uses **even** StreamIDs if it ever initiates streams. In this
  protocol the server **MUST NOT** initiate streams, so a conformant server only
  ever *accepts* odd StreamIDs and **MUST** treat an even inbound SYN as a
  protocol error.
- StreamIDs **MUST** be monotonically increasing per side and are never reused
  within a session.

### 4.5 Flow control

- Each stream has an independent receive window. There is **no** session-level
  window.
- The **initial** receive window per stream is **256 KiB** (262144 bytes). This
  is fixed by yamux and applies before any Window Update.
- A receiver advertises additional capacity by sending a **Window Update** frame
  whose Length is the delta (bytes) to add to the sender's window for that
  StreamID.
- A sender **MUST NOT** send more Data bytes on a stream than its current window
  allows; it decrements the window by each Data payload's byte count and waits
  for Window Updates to replenish it.
- **Window auto-tuning (profile).** HashiCorp yamux grows a stream's receive
  window beyond the 256 KiB initial value, up to a configured maximum, when it
  observes fast consumption. The reference client/server set this maximum
  (`MaxStreamWindowSize`) **per-transport** (see §7): **16 MiB** on the HTTP
  dual-channel path, where each Window Update costs a full HTTP round-trip and a
  large window is needed to keep bulk transfers flowing, and **256 KiB** on the
  WebSocket path, where the low RTT makes a large window unnecessary and a small
  one keeps per-stream memory low. A conformant server:
  - **MUST** honor Window Updates it receives (allowing the peer's window to grow
    up to the peer's configured maximum), and
  - **SHOULD** itself send Window Updates as it drains stream data so bulk
    transfers (e.g. large SQL result sets) are not serialized into many
    round-trips. A server that never grows its receive window past 256 KiB is
    still *correct* but will be slow for bulk downloads.
  - **MUST NOT** assume the peer implements identical auto-tuning heuristics;
    interoperability depends only on honoring received Window Updates and the
    256 KiB floor, not on matching the growth algorithm.

> Implementation note: libp2p-flavored yamux libraries implement the same base
> framing but differ in window auto-tuning defaults. Prefer a HashiCorp-compatible
> library, and validate with the bulk-transfer conformance test (§8) rather than
> trusting the label.

### 4.6 Keepalive (Ping)

- The reference client and server enable keepalive with a **30-second** interval.
- A keepalive is a **Ping** frame (Type `0x2`, StreamID `0x0`) with the SYN flag
  and an opaque Length value.
- On receiving a Ping with SYN, a peer **MUST** reply with a Ping carrying the
  ACK flag and the **same** opaque value.
- A server **SHOULD** send its own periodic Pings and **SHOULD** tear the session
  down if a Ping is not answered within a reasonable timeout, to reclaim dead
  sessions.

### 4.7 Session termination (Go Away)

- A **Go Away** frame (Type `0x3`, StreamID `0x0`) signals the session is
  terminating; the Length field carries an error code:

  | Code | Meaning |
  |------|---------|
  | `0x0` | Normal termination |
  | `0x1` | Protocol error |
  | `0x2` | Internal error |

- After sending or receiving Go Away, an endpoint **MUST NOT** open new streams
  and **SHOULD** drain and close existing streams.

### 4.8 Stream lifecycle summary

1. **Open** — client sends the first Data (or Window Update) frame for a new odd
   StreamID with the **SYN** flag. Data **MAY** be sent before the ACK is
   received.
2. **Accept** — server replies with the **ACK** flag (typically on its first
   Data/Window Update for that stream), or rejects with **RST**.
3. **Transfer** — bidirectional Data frames, bounded by each side's flow-control
   window.
4. **Half-close** — either side sends a frame with **FIN**; it will send no more
   Data but **MAY** still receive.
5. **Close** — the stream is fully closed once both sides have sent FIN, or
   immediately on **RST**.

---

## 5. Application framing — OpenRequest

Immediately after a yamux stream is opened, and before any other application
bytes, the **client** sends exactly one **OpenRequest** as the first bytes of the
stream. The server **MUST** read it before dialing.

### 5.1 Wire format

```
+------------------+---------------------------+
| length (4 bytes) | JSON payload (length bytes)|
+------------------+---------------------------+
```

- **length**: unsigned 32-bit integer, **big-endian**, = number of JSON bytes
  that follow.
- **JSON payload**: a UTF-8 JSON object:

```json
{ "network": "tcp", "address": "host:port" }
```

| Field | Type | Rules |
|-------|------|-------|
| `network` | string | Optional on the wire; defaults to `"tcp"`. The server **MUST** reject any value other than `"tcp"`. |
| `address` | string | Required, non-empty. A `host:port` target the server will dial. |

### 5.2 Validation (server)

The server **MUST**:

- Read a 4-byte big-endian length prefix, then exactly that many JSON bytes.
- Reject the stream (close it; **MAY** send RST) if:
  - length is `0` or greater than **4096** (`MaxOpenRequestSize`), or
  - the JSON fails to parse, or
  - `network` is present and not `"tcp"`, or
  - `address` is empty.
- After a valid OpenRequest, dial the target (`tcp`, `address`). On dial failure
  the server **MUST** close the stream (the client observes EOF); it **MUST NOT**
  send any error payload — the byte stream is transparent.

### 5.3 Data path

After a successful dial, the server **MUST** splice bytes transparently in both
directions between the yamux stream and the dialed target connection until either
side closes, then close both. No further framing is applied — everything after
the OpenRequest is opaque application data (e.g. the PostgreSQL wire protocol).

---

## 6. Session and resource limits

These limits are how the reference server protects a memory-constrained pod. A
conformant server **MAY** implement them; the values are what the reference
deployment uses and are surfaced here so alternative servers can adopt compatible
behavior.

| Limit | Env var | Reject behavior |
|-------|---------|-----------------|
| Max concurrent sessions (WS + HTTP) | `TUNNEL_MAX_SESSIONS` (0 = unlimited) | HTTP `503 Service Unavailable` at connect time. |
| Max concurrent proxied streams (all sessions) | `TUNNEL_MAX_STREAMS` (0 = unlimited) | Accept then immediately close the over-limit stream. |
| Per-stream receive window — HTTP dual-channel | `TUNNEL_STREAM_WINDOW` (default 16 MiB, min 256 KiB) | Governs yamux `MaxStreamWindowSize` (§4.5) on the HTTP path; large to amortize round-trips. |
| Per-stream receive window — WebSocket | `TUNNEL_WS_STREAM_WINDOW` (default 256 KiB, min 256 KiB) | Governs yamux `MaxStreamWindowSize` (§4.5) on the WS path; small to bound per-stream memory. |
| Per-direction throughput cap | `TUNNEL_STREAM_RATE` (0 = unlimited) | Token-bucket rate limit per stream direction. |
| HTTP dual-channel enabled | `TUNNEL_HTTP_FALLBACK` (default **disabled**; `true`/`1` to enable) | When disabled, the server replies `404` to `/tunnels/down` and `/tunnels/up` (§A.2/§A.3) and the client will not use the HTTP transport. Enable on **both** ends to use it. |

Rejections **MUST** be applied at connect/stream-open time so the client can back
off; the client retries with exponential backoff and jitter.

The client-side reconnect profile (informative): initial backoff 500 ms, doubling
to a 15 s cap, each wait multiplied by a random factor in `[0.5, 1.5)`. A server
restart therefore sees clients reconnect spread over time rather than in lockstep.

---

## 7. Configuration constants (normative for interop)

Both endpoints **MUST** agree on the framing constants; the tunable window is
negotiated dynamically via Window Updates. The maximum is **per-transport**: the
two ends of a given session SHOULD target the same maximum for that transport
(large on the HTTP dual-channel path, small on WebSocket — see §4.5).

| Parameter | Value | Notes |
|-----------|-------|-------|
| yamux Version | `0` | §4.1 |
| Initial stream window | 256 KiB (262144 B) | Fixed by yamux (§4.5) |
| Max stream window — HTTP dual-channel | 16 MiB (default) | `TUNNEL_STREAM_WINDOW`; large window amortizes the HTTP round-trip |
| Max stream window — WebSocket | 256 KiB (default) | `TUNNEL_WS_STREAM_WINDOW`; small window bounds per-stream memory on the low-RTT path |
| Keepalive interval | 30 s | Ping (§4.6) |
| Connection write timeout | 30 s | Local send deadline; not on the wire |
| OpenRequest max size | 4096 bytes | §5.2 |
| WebSocket message type | Binary (`0x2`) | §3.2 |
| Copy buffer size (client) | 32 KiB | Informative; bounds WS frame size |

---

## 8. Conformance checklist

A candidate server is conformant if, driven by the reference Go client
(`cmd/tunnel-client`), it satisfies all of the following:

- [ ] **Auth** — rejects a bad/missing Bearer key with `401`; admits a valid key.
- [ ] **WS upgrade** — completes an RFC 6455 upgrade at `GET /tunnels`, binary
      frames only, no Origin requirement.
- [ ] **yamux server** — accepts odd-numbered SYN streams, replies ACK; never
      initiates streams; treats an even inbound SYN as a protocol error.
- [ ] **Flow control** — advertises a 256 KiB initial window; honors inbound
      Window Updates up to the peer's configured maximum (16 MiB on HTTP,
      256 KiB on WebSocket); sends Window Updates as it drains.
- [ ] **Keepalive** — answers Ping(SYN) with Ping(ACK) echoing the opaque value.
- [ ] **OpenRequest** — reads the 4-byte length + JSON; enforces `network == tcp`,
      non-empty `address`, size ≤ 4096; rejects malformed requests.
- [ ] **Dial + splice** — dials the target and transparently proxies bytes both
      ways; closes both sides when either closes.
- [ ] **Teardown** — tearing down the WebSocket closes the yamux session and all
      streams; a dropped target connection closes the corresponding stream only.

### 8.1 Recommended interop test

The single most valuable test — it catches the flow-control bugs that a naive
yamux port exhibits:

1. Configure the Go client with a forward `localhost:15432 -> <target>:5432`
   pointing at the candidate server.
2. Open **N ≥ 8** concurrent local connections; on each, transfer a payload
   **larger than 1 MiB** in each direction.
3. Assert: no stall/deadlock (proves Window Update handling past the 256 KiB
   initial window), byte-exact delivery, and stable server memory.

A server that hangs after ~256 KiB on a single stream has broken flow control
(missing or incorrect Window Updates) — the most common interop failure.

---

## Appendix A — Transport B: HTTP dual-channel (OPTIONAL)

This transport exists only because some gateways (e.g. APISIX in the reference
deployment) block WebSocket upgrades. It emulates one full-duplex byte stream —
the exact same stream yamux runs on in §3 — using two half-duplex HTTP channels.
It has higher per-frame latency (each UP batch is a full HTTP round-trip) and is a
fallback, not the preferred path. A server **MAY** omit it if its gateway permits
WebSocket.

In the reference deployment this transport is **disabled by default** and gated by
`TUNNEL_HTTP_FALLBACK` (§6): the server serves `/tunnels/down` and `/tunnels/up`
(§A.2/§A.3) only when it is enabled, replying `404` otherwise, and the reference
client will not fall back to (or directly dial) HTTP unless it is enabled. Enable
the flag on **both** ends for a gateway that blocks WebSocket.

Upward of this layer everything is identical: the reassembled byte stream carries
yamux (§4), which carries OpenRequests (§5). Only the bottom byte-transport
differs.

### A.1 Session identifier

The client generates a random 128-bit **session id** (`sid`), lowercase hex
(32 chars), and uses it to correlate the two channels. Both channels for one
logical session **MUST** carry the same `sid`.

### A.2 DOWN channel (server → client)

```
GET /tunnels/down?sid=<sid>
Authorization: Bearer <api-key>
```

- The server **MUST** authenticate, then register `sid`. If a live DOWN stream
  already exists for that `sid`, it **MUST** reject with `409 Conflict` (prevents
  a duplicate stream from hijacking an active session).
- On success the server responds `200 OK` with:
  - `Content-Type: application/octet-stream`
  - `Cache-Control: no-cache, no-store`
  - `X-Accel-Buffering: no` (disables proxy buffering)
  - a **chunked**, unbounded response body that stays open for the session's
    lifetime.
- The server writes **server→client yamux frames** into this body and **MUST
  flush** after each write so the proxy forwards bytes immediately.
- The server **MUST** clear/disable any HTTP write deadline for this response so
  a `WriteTimeout` does not kill the long-lived stream.
- When the DOWN request ends (client disconnect) or the session closes, the
  server **MUST** tear the session down.

### A.3 UP channel (client → server)

```
POST /tunnels/up?sid=<sid>&seq=<N>
Authorization: Bearer <api-key>
Content-Type: application/octet-stream

<body: one batch of client→server yamux frames>
```

- Each POST carries one batch of client→server yamux bytes tagged with a
  **monotonically increasing** `seq` starting at `0`.
- The client **MAY** pipeline up to **4** UP POSTs concurrently (they multiplex
  over one HTTP/2 connection). Because HTTP/2 does not guarantee delivery order,
  the server **MUST reorder** batches by `seq` before feeding them to yamux, so
  yamux observes a correctly ordered byte stream.
- The server **MUST**:
  - Reject an unknown `sid` with `404 Not Found`.
  - Reject a body larger than **4 MiB** (`maxUpBodyBytes`) with `413 Request
    Entity Too Large`.
  - Buffer the batch under its `seq`, then respond `200 OK` **immediately**
    (before the bytes are drained) so the client can pipeline the next POST.
  - Bound the reorder buffer to **8 MiB** (`maxPendingBytes`) of not-yet-applied
    batches. If a new batch would exceed this, the server **MUST** tear the
    session down and respond `507 Insufficient Storage` (protects against a
    client that pipelines faster than the session drains, or abuses out-of-order
    `seq`).

### A.4 Reassembly model

The server presents the pair (DOWN response writer, ordered UP feed) to yamux as a
single `io.ReadWriteCloser`:

- **Read** returns bytes fed by UP POSTs in `seq` order (a feeder applies
  `seq = nextSeq` consecutively; gaps wait until the missing `seq` arrives).
- **Write** emits bytes onto the DOWN response and flushes.

yamux runs on this composite stream unchanged.

### A.5 Client URL scheme selection (informative)

The reference client picks a transport from the configured `server_url` scheme:

HTTP dual-channel is only used when `TUNNEL_HTTP_FALLBACK` is enabled (§6);
disabled by default.

| Scheme | Behavior |
|--------|----------|
| `wss://` / `ws://` | Try WebSocket (§3) first. On **any** failure (e.g. `426` from a WS-blocking proxy): fall back to HTTP dual-channel **if** `TUNNEL_HTTP_FALLBACK` is enabled; otherwise return the error and retry WebSocket with backoff. Re-evaluated on every reconnect, so WS resumes automatically if the gateway is later fixed. |
| `https://` / `http://` | Use HTTP dual-channel directly; no WebSocket attempt. Requires `TUNNEL_HTTP_FALLBACK` enabled — otherwise the client errors at startup. |

---

## Appendix B — Client configuration (informative)

The reference client is configured with JSON:

```json
{
  "server_url": "wss://gateway.example.com/tunnels",
  "api_key": "<api-key>",
  "forwards": [
    { "local": "127.0.0.1:15432", "remote": "10.0.0.5:5432" }
  ]
}
```

- `server_url` — tunnel endpoint; scheme selects transport (Appendix A.5). For
  HTTP transport the base path is the `/tunnels` prefix; the client appends
  `/down` and `/up`.
- `api_key` — Bearer token (§2).
- `forwards[]` — each entry opens a persistent local listener on `local` and
  tunnels accepted connections to `remote` (the OpenRequest `address`, §5).
  Listeners survive tunnel reconnects; only in-flight tunneled connections are
  lost on a drop.

---

## Appendix C — WebSocket gateway relay (informative)

This appendix describes a deployment topology, not a new wire protocol. It exists
for environments where only a single edge component is permitted to accept
WebSocket upgrades (e.g. an APISIX-fronted Go gateway in the reference
deployment), while the actual tunnel **server** (§1.1) runs deeper in the network
and is reachable only over plain TCP.

```
Client ──ws://──► Gateway (terminates WS, authenticates) ──raw TCP──► Upstream server
        WebSocket        edge auth (§2), no yamux parsing        yamux directly on socket
```

### C.1 Role of the gateway

- The gateway is the **only** component that accepts a WebSocket upgrade
  (§3). It terminates the client WebSocket and authenticates the Bearer key (§2)
  at the edge, exactly as a §3 server would.
- After authenticating, the gateway relays the reassembled binary byte stream
  **unchanged** to an internal endpoint over a raw TCP connection. It is a
  transparent byte pipe: it **MUST NOT** parse, buffer per-frame, or terminate
  yamux (§4) — the yamux session runs **end-to-end** between the client and the
  internal upstream server. The wire protocol above the transport is therefore
  identical to §3/§4/§5; only the bottom transport is split into two hops.
- Because the gateway already authenticated the client, the internal endpoint
  **MAY** trust the network for authentication and omit its own Bearer check.

### C.2 Internal endpoint (upstream server)

- The internal endpoint **MAY** be **plain TCP**: yamux (§4) runs directly on the
  accepted socket, with **no** WebSocket framing. The upstream accepts a TCP
  connection and immediately acts as the yamux **server** (§1.1) on it.
- Both ends of yamux **MUST** use the **WebSocket** window profile — the 256 KiB
  max stream window (§7) — since from yamux's perspective the session is the
  low-RTT WebSocket session that the client negotiated; the gateway's TCP hop is
  invisible to flow control.

### C.3 Lifecycle

1. Client dials the gateway's WebSocket endpoint with the Bearer header.
2. Gateway authenticates (§2), upgrades the WebSocket, and dials the internal
   upstream over TCP.
3. Gateway splices bytes transparently in both directions until either side
   closes, then tears down both hops. A dropped WebSocket closes the upstream TCP
   connection (and thus the yamux session and all streams), and vice versa.
