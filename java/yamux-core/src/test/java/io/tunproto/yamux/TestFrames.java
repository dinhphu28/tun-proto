package io.tunproto.yamux;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.nio.charset.StandardCharsets;

/** Test helpers for building raw wire frames and OpenRequest bytes. */
final class TestFrames {

    static final UnpooledByteBufAllocator ALLOC = UnpooledByteBufAllocator.DEFAULT;

    private TestFrames() {}

    static ByteBuf header(FrameType type, int flags, long streamId, long lenOrDelta) {
        ByteBuf b = Unpooled.buffer(12);
        b.writeByte(0);
        b.writeByte(type.value);
        b.writeShort(flags & 0xFFFF);
        b.writeInt((int) (streamId & 0xFFFFFFFFL));
        b.writeInt((int) (lenOrDelta & 0xFFFFFFFFL));
        return b;
    }

    static ByteBuf data(int flags, long streamId, byte[] payload) {
        ByteBuf b = Unpooled.buffer(12 + payload.length);
        b.writeByte(0);
        b.writeByte(FrameType.DATA.value);
        b.writeShort(flags & 0xFFFF);
        b.writeInt((int) (streamId & 0xFFFFFFFFL));
        b.writeInt(payload.length);
        b.writeBytes(payload);
        return b;
    }

    static byte[] openRequestBytes(String address) {
        String json = "{\"network\":\"tcp\",\"address\":\"" + address + "\"}";
        byte[] jb = json.getBytes(StandardCharsets.UTF_8);
        ByteBuf b = Unpooled.buffer(4 + jb.length);
        b.writeInt(jb.length);
        b.writeBytes(jb);
        byte[] out = new byte[b.readableBytes()];
        b.readBytes(out);
        b.release();
        return out;
    }
}
