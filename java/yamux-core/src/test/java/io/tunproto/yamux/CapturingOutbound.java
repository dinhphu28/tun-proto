package io.tunproto.yamux;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/** OutboundHandler that decodes and records every emitted frame, releasing buffers. */
final class CapturingOutbound implements OutboundHandler {

    static final class Rec {
        final FrameType type;
        final int flags;
        final long streamId;
        final long length;
        final byte[] payload;

        Rec(FrameType type, int flags, long streamId, long length, byte[] payload) {
            this.type = type;
            this.flags = flags;
            this.streamId = streamId;
            this.length = length;
            this.payload = payload;
        }
    }

    final List<Rec> frames = new ArrayList<>();

    @Override
    public void onFrame(ByteBuf frame) {
        try {
            int ri = frame.readerIndex();
            FrameType type = FrameType.from(FrameCodec.type(frame, ri));
            int flags = FrameCodec.flags(frame, ri);
            long sid = FrameCodec.streamId(frame, ri);
            long len = FrameCodec.length(frame, ri);
            byte[] payload = null;
            if (type == FrameType.DATA) {
                payload = new byte[(int) len];
                frame.getBytes(ri + YamuxConstants.HEADER_LEN, payload, 0, (int) len);
            }
            frames.add(new Rec(type, flags, sid, len, payload));
        } finally {
            frame.release();
        }
    }

    long totalDataBytes(long streamId) {
        long sum = 0;
        for (Rec r : frames) {
            if (r.type == FrameType.DATA && r.streamId == streamId && r.payload != null) {
                sum += r.payload.length;
            }
        }
        return sum;
    }

    int count(FrameType type) {
        int c = 0;
        for (Rec r : frames) {
            if (r.type == type) {
                c++;
            }
        }
        return c;
    }

    void clear() {
        frames.clear();
    }
}
