package io.tunproto.yamux;

/**
 * Protocol constants for yamux v0 over a binary WebSocket. See SPEC.md §4.
 */
public final class YamuxConstants {
    /** yamux protocol version. Always 0. */
    public static final byte VERSION = 0;
    /** 12-byte frame header length: version(1) type(1) flags(2) streamID(4) length(4). */
    public static final int HEADER_LEN = 12;
    /** Stream id 0 is the session stream (ping/goaway). */
    public static final long SESSION_STREAM_ID = 0L;
    /** Initial per-stream window: 256 KiB (yamux floor). */
    public static final int INITIAL_WINDOW = 262_144;
    /** Default max per-stream window: 16 MiB. */
    public static final int DEFAULT_MAX_WINDOW = 16_777_216;
    /** Max DATA payload we emit per frame: 64 KiB. */
    public static final int MAX_FRAME_PAYLOAD = 65_536;
    /** Max OpenRequest JSON length (SPEC §5). */
    public static final int MAX_OPEN_REQUEST_SIZE = 4096;

    /** GO_AWAY error codes. */
    public static final long GOAWAY_NORMAL = 0;
    public static final long GOAWAY_PROTO = 1;
    public static final long GOAWAY_INTERNAL = 2;

    private YamuxConstants() {}
}
