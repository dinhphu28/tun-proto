package io.tunproto.vertx.internal;

import io.netty.buffer.ByteBuf;
import io.vertx.core.buffer.Buffer;

/**
 * Netty {@link ByteBuf} &lt;-&gt; Vert.x {@link Buffer} bridging helpers.
 *
 * <p>Vert.x {@code Buffer} is itself a thin wrapper over a Netty {@code ByteBuf};
 * {@code Buffer.buffer(ByteBuf)} wraps without copying, and {@code Buffer.getByteBuf()}
 * exposes the underlying (a duplicate view). This lets us move bytes between the
 * yamux engine and the WebSocket/NetSocket without extra copies, while honoring
 * the "release AFTER the write completes" ownership rule of
 * {@code OutboundHandler.onFrame}.</p>
 */
final class BufferBridge {

    private BufferBridge() {
    }

    /** Wrap a Netty ByteBuf into a Vert.x Buffer without copying. */
    static Buffer wrap(ByteBuf buf) {
        return Buffer.buffer(buf);
    }
}
