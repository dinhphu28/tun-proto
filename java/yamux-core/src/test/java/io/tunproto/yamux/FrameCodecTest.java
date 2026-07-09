package io.tunproto.yamux;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FrameCodecTest {

    private final UnpooledByteBufAllocator alloc = UnpooledByteBufAllocator.DEFAULT;

    @Test
    void headerRoundTrip() {
        int[] flagsCases = {0, Flags.SYN, Flags.ACK, Flags.FIN, Flags.RST, Flags.SYN | Flags.ACK};
        for (FrameType t : FrameType.values()) {
            for (int flags : flagsCases) {
                long sid = 0xFFFFFFFFL;
                long len = 0xFFFFFFFFL;
                ByteBuf b = FrameCodec.encodeHeader(alloc, t, flags, sid, len);
                assertEquals(12, b.readableBytes());
                assertEquals(0, FrameCodec.version(b, 0));
                assertEquals(t.value, FrameCodec.type(b, 0));
                assertEquals(flags, FrameCodec.flags(b, 0));
                assertEquals(sid, FrameCodec.streamId(b, 0));
                assertEquals(len, FrameCodec.length(b, 0));
                // Big-endian hand-check of the first bytes.
                assertEquals(0, b.getByte(0));
                assertEquals(t.value, b.getByte(1));
                b.release();
            }
        }
    }

    @Test
    void dataFrameRoundTrip() {
        byte[] payload = "hello-world".getBytes();
        ByteBuf p = Unpooled.wrappedBuffer(payload);
        ByteBuf b = FrameCodec.encodeData(alloc, Flags.ACK, 3, p);
        assertEquals(0, FrameCodec.version(b, 0));
        assertEquals(FrameType.DATA.value, FrameCodec.type(b, 0));
        assertEquals(Flags.ACK, FrameCodec.flags(b, 0));
        assertEquals(3, FrameCodec.streamId(b, 0));
        assertEquals(payload.length, FrameCodec.length(b, 0));
        byte[] out = new byte[payload.length];
        b.getBytes(12, out, 0, payload.length);
        assertArrayEquals(payload, out);
        b.release();
    }
}
