package io.tunproto.yamux;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

/**
 * Pure, sans-IO yamux frame codec. Big-endian, 12-byte header:
 * version(1) type(1) flags(2) streamID(4) length(4).
 * StreamID and length are unsigned 32-bit values read into a {@code long}.
 */
public final class FrameCodec {

    private FrameCodec() {}

    /**
     * Encode a 12-byte header-only frame (WINDOW_UPDATE / PING / GO_AWAY).
     * Ownership of the returned buffer transfers to the caller.
     */
    public static ByteBuf encodeHeader(ByteBufAllocator alloc, FrameType t, int flags,
                                       long streamId, long lengthOrDeltaOrOpaque) {
        ByteBuf b = alloc.buffer(YamuxConstants.HEADER_LEN, YamuxConstants.HEADER_LEN);
        b.writeByte(YamuxConstants.VERSION);
        b.writeByte(t.value);
        b.writeShort(flags & 0xFFFF);
        b.writeInt((int) (streamId & 0xFFFFFFFFL));
        b.writeInt((int) (lengthOrDeltaOrOpaque & 0xFFFFFFFFL));
        return b;
    }

    /**
     * Encode a DATA frame: 12-byte header (length = payload readable bytes) followed
     * by the payload bytes. The payload is consumed (its reader index advances) and
     * released; ownership of the returned buffer transfers to the caller.
     */
    public static ByteBuf encodeData(ByteBufAllocator alloc, int flags, long streamId, ByteBuf payload) {
        int len = payload.readableBytes();
        ByteBuf b = alloc.buffer(YamuxConstants.HEADER_LEN + len);
        try {
            b.writeByte(YamuxConstants.VERSION);
            b.writeByte(FrameType.DATA.value);
            b.writeShort(flags & 0xFFFF);
            b.writeInt((int) (streamId & 0xFFFFFFFFL));
            b.writeInt(len);
            b.writeBytes(payload, payload.readerIndex(), len);
        } finally {
            payload.release();
        }
        return b;
    }

    public static int version(ByteBuf b, int idx) {
        return b.getByte(idx) & 0xFF;
    }

    public static int type(ByteBuf b, int idx) {
        return b.getByte(idx + 1) & 0xFF;
    }

    public static int flags(ByteBuf b, int idx) {
        return b.getShort(idx + 2) & 0xFFFF;
    }

    public static long streamId(ByteBuf b, int idx) {
        return b.getInt(idx + 4) & 0xFFFFFFFFL;
    }

    public static long length(ByteBuf b, int idx) {
        return b.getInt(idx + 8) & 0xFFFFFFFFL;
    }
}
