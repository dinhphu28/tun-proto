package io.tunproto.yamux;

import io.netty.buffer.ByteBuf;

/**
 * Immutable value object for a decoded frame. Used mainly by tests; the session
 * engine parses frames inline for efficiency.
 */
public final class Frame {
    public final FrameType type;
    public final int flags;
    public final long streamId;
    /** length / delta / opaque / errcode depending on type. */
    public final long length;
    /** DATA payload (retained slice), else null. */
    public final ByteBuf payload;

    public Frame(FrameType type, int flags, long streamId, long length, ByteBuf payload) {
        this.type = type;
        this.flags = flags;
        this.streamId = streamId;
        this.length = length;
        this.payload = payload;
    }
}
