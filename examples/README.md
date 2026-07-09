# Examples

Concrete, byte-exact examples for the framing described in
[`../SPEC.md`](../SPEC.md).

## Files

| File | What it is |
|------|-----------|
| `open-request.json` | An example OpenRequest payload (§5). |
| `open-request.framed.bin` | The exact bytes a client writes as the first bytes of a yamux stream: a 4-byte big-endian length prefix followed by the JSON. |
| `yamux-frames.md` | Annotated example yamux frame headers (SYN, Window Update, Ping) — §4. |

## OpenRequest (§5)

The payload:

```json
{"network":"tcp","address":"10.0.0.5:5432"}
```

On the wire this is **length-prefixed**. The JSON is 43 bytes, so the client
sends 47 bytes total:

```
00000000: 0000 002b 7b22 6e65 7477 6f72 6b22 3a22  ...+{"network":"
00000010: 7463 7022 2c22 6164 6472 6573 7322 3a22  tcp","address":"
00000020: 3130 2e30 2e30 2e35 3a35 3433 3222 7d    10.0.0.5:5432"}
```

Breakdown:

| Bytes | Meaning |
|-------|---------|
| `00 00 00 2b` | uint32 big-endian length = **43** (JSON byte count) |
| `7b ... 7d` | the 43 UTF-8 JSON bytes |

> The canonical wire form of the JSON has **no trailing newline**. The
> `open-request.json` file in this directory ends with a newline for text-editor
> friendliness; `open-request.framed.bin` is the authoritative byte sequence.

### Server-side handling

1. `io.ReadFull` a 4-byte prefix → length `N`. Reject if `N == 0` or `N > 4096`.
2. `io.ReadFull` exactly `N` bytes → parse JSON.
3. Validate: `network` absent or `"tcp"`; `address` non-empty.
4. Dial `tcp` / `address`, then splice bytes transparently (everything after the
   OpenRequest is opaque — e.g. the PostgreSQL wire protocol).

### Verifying the framed bytes yourself

```sh
# Regenerate and compare
python3 - <<'PY'
import json, struct
payload = b'{"network":"tcp","address":"10.0.0.5:5432"}'
frame = struct.pack(">I", len(payload)) + payload
open("/tmp/expected.bin", "wb").write(frame)
print("len(json) =", len(payload), " len(frame) =", len(frame))
PY
cmp /tmp/expected.bin open-request.framed.bin && echo "OK: bytes match"
```
