# Interop Test

The single most valuable conformance test. It exercises yamux **flow control**
past the 256 KiB initial window (§4.5 of [`../SPEC.md`](../SPEC.md)) — the
behavior a naive yamux port most often gets wrong. A server that passes this is
almost certainly interoperable; one that hangs after ~256 KiB on a stream has
broken Window Update handling.

## What it proves

1. **Stream open + OpenRequest parsing** — the client can open a stream and the
   server dials the named target.
2. **Flow control past 256 KiB** — a payload larger than the initial window
   flows without stalling, proving the server sends/honors Window Updates.
3. **Concurrency** — many simultaneous streams over one transport connection stay
   independent and correct.
4. **Byte-exactness** — data is delivered intact in both directions.
5. **Memory stability** — the server does not blow its heap under concurrent bulk
   streams.

## Setup

You need three things:

1. A **candidate server** implementing this spec, reachable at a URL.
2. A **reference client** (e.g. the Go `tunnel-client`) configured to forward a
   local port to a target *through* the candidate server.
3. A **target** the server can dial — for a clean byte-echo test, any TCP echo
   service works (e.g. `ncat -l -k -e /bin/cat 5432`, or a real PostgreSQL).

```
[test driver] ──► localhost:15432 ──► [reference client] ──tunnel──► [candidate server] ──► [target:5432]
```

### Sample client config

See [`sample-client-config.json`](./sample-client-config.json). Point
`server_url` at the candidate server and `forwards[].remote` at the target.

```json
{
  "server_url": "wss://candidate.example.com/tunnels",
  "api_key": "<api-key>",
  "forwards": [
    { "local": "127.0.0.1:15432", "remote": "10.0.0.5:5432" }
  ]
}
```

## Procedure

1. Start the candidate server and the target echo service.
2. Start the reference client with the config above; confirm it logs a successful
   tunnel connection.
3. Open **N ≥ 8** concurrent TCP connections to `127.0.0.1:15432`.
4. On each connection, send a payload **larger than 1 MiB** (well past the 256 KiB
   initial window), then read the echoed bytes back.
5. Record wall-clock, bytes in/out, and server RSS during the run.

### Reference driver (against a TCP echo target)

```sh
# 8 concurrent 4 MiB round-trips through the tunnel to an echo server.
set -e
head -c 4194304 /dev/urandom > /tmp/payload.bin
sha_in=$(sha256sum /tmp/payload.bin | cut -d' ' -f1)

pids=""
for i in $(seq 1 8); do
  ( nc 127.0.0.1 15432 < /tmp/payload.bin > /tmp/echo_$i.bin ) &
  pids="$pids $!"
done
for p in $pids; do wait "$p"; done

fail=0
for i in $(seq 1 8); do
  sha_out=$(sha256sum /tmp/echo_$i.bin | cut -d' ' -f1)
  if [ "$sha_out" != "$sha_in" ]; then echo "stream $i MISMATCH"; fail=1; fi
done
[ "$fail" = 0 ] && echo "PASS: 8/8 streams byte-exact" || echo "FAIL"
```

> `nc` half-close behavior varies by implementation; if your `nc` doesn't send
> FIN after stdin EOF, use `ncat --send-only`/`--recv-only` pairs or a small
> socket script instead. The assertion that matters is byte-exact echo of a
> >1 MiB payload on every concurrent stream.

## Pass criteria

- [ ] **No stall / deadlock.** Every stream completes; none hang after ~256 KiB.
- [ ] **Byte-exact.** Echoed bytes match the sent payload on all N streams
      (checksum equality).
- [ ] **Concurrency.** All N streams run over a single transport connection and
      complete independently.
- [ ] **Memory stable.** Server RSS stays within its configured budget; no OOM.

## Interpreting failures

| Symptom | Likely cause |
|---------|--------------|
| Stream hangs at ~256 KiB total | Server never sends Window Updates (broken flow control). |
| Hang only under load / large windows | Window auto-tuning mismatch; server ignores large inbound Window Updates. |
| Truncated / corrupted bytes | Treating one WS message as one yamux frame, or mis-parsing the 12-byte header. |
| Works single-stream, fails at N>1 | StreamID handling / per-stream state not isolated. |
| Server OOM under concurrency | No per-stream window bound or stream cap (§6). |
