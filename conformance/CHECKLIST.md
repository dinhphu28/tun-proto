# Conformance Checklist

A candidate **server** is conformant with [`../SPEC.md`](../SPEC.md) if, driven by
a reference client, it satisfies every item below. Section references point into
`SPEC.md`.

## Required

### Authentication (§2)
- [ ] Rejects a missing `Authorization` header with `401`.
- [ ] Rejects an invalid Bearer key with `401`, before any yamux processing.
- [ ] Admits a valid Bearer key.

### WebSocket transport (§3)
- [ ] Completes an RFC 6455 upgrade at `GET /tunnels`.
- [ ] Uses **binary** frames only; never text frames.
- [ ] Does not reject a request for a missing/foreign `Origin` header.
- [ ] Treats the yamux byte stream as continuous (does not assume 1 WS message = 1 yamux frame).
- [ ] Read limit (if any) ≥ negotiated max stream window, so large Data frames are not dropped.

### yamux — server role (§4)
- [ ] Acts strictly as the yamux **server**: accepts streams, never initiates them.
- [ ] Accepts **odd**-numbered SYN streams; replies ACK.
- [ ] Treats an **even**-numbered inbound SYN as a protocol error.
- [ ] Parses the 12-byte big-endian header correctly (Version `0`, Type, Flags, StreamID, Length).
- [ ] Honors FIN (half-close) and RST (hard reset) semantics.

### yamux — flow control (§4.5)  ← most common failure point
- [ ] Advertises a **256 KiB** initial receive window.
- [ ] Honors inbound Window Updates, allowing the peer window to grow to **≥ 16 MiB**.
- [ ] Sends Window Updates as it drains stream data (does not stall bulk transfers at 256 KiB).
- [ ] Only Data payload bytes decrement the window; control frames do not.

### yamux — keepalive & teardown (§4.6, §4.7)
- [ ] Answers `Ping(SYN)` with `Ping(ACK)` echoing the opaque value.
- [ ] Handles `GoAway`; stops opening streams after send/receive.

### OpenRequest (§5)
- [ ] Reads a 4-byte big-endian length prefix, then exactly that many JSON bytes.
- [ ] Rejects length `0` or `> 4096`.
- [ ] Rejects malformed JSON.
- [ ] Rejects `network` values other than `"tcp"` (absent defaults to `"tcp"`).
- [ ] Rejects empty `address`.

### Dial + splice (§5.3)
- [ ] Dials the named `tcp` target after a valid OpenRequest.
- [ ] On dial failure, closes the stream with no error payload (byte stream stays transparent).
- [ ] Splices bytes transparently in both directions.
- [ ] Closes both sides when either side closes.

### Teardown (§3.4)
- [ ] Tearing down the WebSocket closes the yamux session and all its streams.
- [ ] A dropped target connection closes only its corresponding stream.

## Optional

### Resource limits (§6)
- [ ] Enforces a max-concurrent-session cap → `503` at connect time.
- [ ] Enforces a max-concurrent-stream cap → over-limit stream closed immediately.
- [ ] Supports a configurable per-stream window (default 16 MiB, min 256 KiB).
- [ ] Supports an optional per-direction throughput cap.

### HTTP dual-channel transport (Appendix A)
- [ ] `GET /tunnels/down?sid=` → `200` chunked octet-stream, flushed per write, no write deadline.
- [ ] Duplicate DOWN for a live `sid` → `409`.
- [ ] `POST /tunnels/up?sid=&seq=` reorders batches by `seq` before feeding yamux.
- [ ] Unknown `sid` → `404`; body `> 4 MiB` → `413`; reorder buffer `> 8 MiB` → `507` + session torn down.
- [ ] Responds `200` immediately after buffering an UP batch (before draining).

---

See [`interop-test.md`](./interop-test.md) for the end-to-end procedure that
exercises the flow-control items — the ones a naive yamux port gets wrong.
