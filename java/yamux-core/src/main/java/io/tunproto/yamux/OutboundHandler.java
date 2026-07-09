package io.tunproto.yamux;

import io.netty.buffer.ByteBuf;

/**
 * Sink for encoded outbound frames. Ownership of {@code frame} transfers to the
 * handler: it MUST write the buffer and then release it AFTER the write completes
 * (never synchronously with a pooled allocator).
 */
@FunctionalInterface
public interface OutboundHandler {
    void onFrame(ByteBuf frame);
}
