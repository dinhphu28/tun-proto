package io.tunproto.yamux;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Proves lesson #1: applying the SYN window delta prevents the >256 KiB deadlock.
 */
class FlowControlSynDeltaTest {

    private static final int SYN_DELTA = 16515072; // 16 MiB - 256 KiB

    private YamuxSession newSession(CapturingOutbound out, AtomicReference<YamuxStream> ref) {
        YamuxConfig cfg = YamuxConfig.builder().allocator(TestFrames.ALLOC).build();
        return new YamuxSession(cfg, out, ref::set);
    }

    private YamuxStream openStream(YamuxSession s, AtomicReference<YamuxStream> ref, int synDelta) {
        // SYN-flagged WINDOW_UPDATE creates the stream and bumps sendWindow.
        ByteBuf syn = TestFrames.header(FrameType.WINDOW_UPDATE, Flags.SYN, 1, synDelta);
        s.receive(syn);
        syn.release();
        // OpenRequest DATA arrives next (kept minimal here; consumed by dataHandler).
        YamuxStream st = ref.get();
        assertNotNull(st);
        st.dataHandler(ByteBuf::release);
        return st;
    }

    @Test
    void largeDownloadDoesNotStallWithSynDelta() {
        CapturingOutbound out = new CapturingOutbound();
        AtomicReference<YamuxStream> ref = new AtomicReference<>();
        YamuxSession s = newSession(out, ref);
        YamuxStream st = openStream(s, ref, SYN_DELTA);

        int total = 1_000_000;
        ByteBuf data = Unpooled.buffer(total);
        data.writeZero(total);
        int accepted = st.write(data);

        assertEquals(total, accepted, "all 1,000,000 bytes must be accepted with the SYN delta applied");
        assertEquals(total, out.totalDataBytes(1));
        // No WINDOW_UPDATE needed to flush this; the SYN delta already granted ~16 MiB.
        assertEquals(0, out.count(FrameType.WINDOW_UPDATE));
        // First outbound DATA carries ACK (lesson #3).
        CapturingOutbound.Rec first = out.frames.stream()
                .filter(r -> r.type == FrameType.DATA).findFirst().orElseThrow();
        assertTrue(Flags.has(first.flags, Flags.ACK), "first DATA frame must carry ACK");
    }

    @Test
    void negativeControlStallsAt256KiBWithoutDelta() {
        CapturingOutbound out = new CapturingOutbound();
        AtomicReference<YamuxStream> ref = new AtomicReference<>();
        YamuxSession s = newSession(out, ref);
        YamuxStream st = openStream(s, ref, 0); // delta 0

        int total = 1_000_000;
        ByteBuf data = Unpooled.buffer(total);
        data.writeZero(total);
        int accepted = st.write(data);

        assertEquals(YamuxConstants.INITIAL_WINDOW, accepted,
                "without the SYN delta only 262144 bytes flow until a WINDOW_UPDATE");
        assertEquals(YamuxConstants.INITIAL_WINDOW, out.totalDataBytes(1));

        // Now feed a WINDOW_UPDATE granting the rest; the remainder flushes.
        ByteBuf wu = TestFrames.header(FrameType.WINDOW_UPDATE, 0, 1, total);
        s.receive(wu);
        wu.release();
        assertEquals(total, out.totalDataBytes(1));
    }
}
