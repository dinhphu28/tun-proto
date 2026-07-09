# Example yamux frames

Annotated examples of the 12-byte yamux frame header (§4 of
[`../SPEC.md`](../SPEC.md)). All fields are big-endian.

```
Offset:  0        1        2   3        4   5   6   7        8   9  10  11
Field:   Version  Type     Flags        StreamID             Length/Delta
```

Constants used below:

- Version = `0x00`
- Type: Data `0x00`, WindowUpdate `0x01`, Ping `0x02`, GoAway `0x03`
- Flags (bitmask): SYN `0x0001`, ACK `0x0002`, FIN `0x0004`, RST `0x0008`

---

## 1. Client opens stream 1 and sends 47 bytes (the OpenRequest)

A Data frame with the **SYN** flag on the first odd StreamID. The 47-byte payload
is the framed OpenRequest from `open-request.framed.bin`.

```
00 00  00 01  00 00 00 01  00 00 00 2f
│  │   │      │            └─ Length = 0x2f = 47 payload bytes follow
│  │   │      └───────────── StreamID = 1 (client → odd)
│  │   └──────────────────── Flags = 0x0001 (SYN)
│  └──────────────────────── Type = 0x00 (Data)
└─────────────────────────── Version = 0x00
```

...immediately followed by the 47 payload bytes:

```
00 00 00 2b 7b 22 6e 65 74 77 6f 72 6b 22 3a 22 74 63 70 22 2c 22
61 64 64 72 65 73 73 22 3a 22 31 30 2e 30 2e 30 2e 35 3a 35 34 33
32 22 7d
```

## 2. Server accepts stream 1

A WindowUpdate frame with the **ACK** flag, granting an initial window delta.
Here Length is a window delta (bytes), not a payload length.

```
00 01  00 02  00 00 00 01  00 04 00 00
│  │   │      │            └─ Delta = 0x00040000 = 262144 (256 KiB)
│  │   │      └───────────── StreamID = 1
│  │   └──────────────────── Flags = 0x0002 (ACK)
│  │
│  └──────────────────────── Type = 0x01 (WindowUpdate)
└─────────────────────────── Version = 0x00
```

> A server MAY instead carry the ACK on its first Data frame for the stream.

## 3. Server grows the receive window during a bulk transfer

As the server drains stream data it advertises more capacity so the sender is not
throttled to the 256 KiB initial window (§4.5). Example: add 16 MiB.

```
00 01  00 00  00 00 00 01  01 00 00 00
│  │   │      │            └─ Delta = 0x01000000 = 16777216 (16 MiB)
│  │   │      └───────────── StreamID = 1
│  │   └──────────────────── Flags = 0x0000 (none)
│  └──────────────────────── Type = 0x01 (WindowUpdate)
└─────────────────────────── Version = 0x00
```

## 4. Keepalive ping (session level)

Sent on StreamID `0`. The opaque value lives in the Length field and MUST be
echoed back with the ACK flag.

Request (SYN):

```
00 02  00 01  00 00 00 00  00 00 12 34
│  │   │      │            └─ opaque value = 0x1234
│  │   │      └───────────── StreamID = 0 (session)
│  │   └──────────────────── Flags = 0x0001 (SYN)
│  └──────────────────────── Type = 0x02 (Ping)
└─────────────────────────── Version = 0x00
```

Reply (ACK) — same opaque value:

```
00 02  00 02  00 00 00 00  00 00 12 34
       └─ Flags = 0x0002 (ACK), StreamID 0, opaque = 0x1234 echoed
```

## 5. Half-close stream 1

Either side may send a Data (or WindowUpdate) frame with the **FIN** flag and no
payload to signal it will send no more data.

```
00 00  00 04  00 00 00 01  00 00 00 00
│  │   │      │            └─ Length = 0 (no payload)
│  │   │      └───────────── StreamID = 1
│  │   └──────────────────── Flags = 0x0004 (FIN)
│  └──────────────────────── Type = 0x00 (Data)
└─────────────────────────── Version = 0x00
```

---

> These byte layouts are illustrative aids for implementers. The authoritative
> framing is the upstream yamux spec
> (<https://github.com/hashicorp/yamux/blob/master/spec.md>); where a real yamux
> library disagrees with an example here, the library/upstream spec governs.
