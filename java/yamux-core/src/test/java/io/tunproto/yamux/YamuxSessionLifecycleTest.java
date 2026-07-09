package io.tunproto.yamux;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class YamuxSessionLifecycleTest {

    private static final int SYN_DELTA = 16515072;

    private YamuxConfig cfg() {
        return YamuxConfig.builder().allocator(TestFrames.ALLOC).build();
    }

    private YamuxSession session(CapturingOutbound out, AtomicReference<YamuxStream> ref) {
        return new YamuxSession(cfg(), out, ref::set);
    }

    private YamuxStream open(YamuxSession s, AtomicReference<YamuxStream> ref, long id) {
        ByteBuf syn = TestFrames.header(FrameType.WINDOW_UPDATE, Flags.SYN, id, SYN_DELTA);
        s.receive(syn);
        syn.release();
        return ref.get();
    }

    @Test
    void pingSynGetsAck() {
        CapturingOutbound out = new CapturingOutbound();
        YamuxSession s = new YamuxSession(cfg(), out, st -> {});
        ByteBuf ping = TestFrames.header(FrameType.PING, Flags.SYN, 0, 0x1234);
        s.receive(ping);
        ping.release();
        assertEquals(1, out.frames.size());
        CapturingOutbound.Rec r = out.frames.get(0);
        assertEquals(FrameType.PING, r.type);
        assertTrue(Flags.has(r.flags, Flags.ACK));
        assertEquals(0x1234, r.length);
    }

    @Test
    void evenSynIsProtocolError() {
        CapturingOutbound out = new CapturingOutbound();
        YamuxSession s = new YamuxSession(cfg(), out, st -> {});
        ByteBuf syn = TestFrames.header(FrameType.WINDOW_UPDATE, Flags.SYN, 2, SYN_DELTA);
        s.receive(syn);
        syn.release();
        // Session tears itself down and emits GO_AWAY(PROTO).
        assertTrue(s.isClosed());
        assertTrue(out.frames.stream().anyMatch(r -> r.type == FrameType.GO_AWAY
                && r.length == YamuxConstants.GOAWAY_PROTO));
    }

    @Test
    void receiverReplenish() {
        CapturingOutbound out = new CapturingOutbound();
        AtomicReference<YamuxStream> ref = new AtomicReference<>();
        // Keep the max window at the 256 KiB floor so the half-window threshold
        // (128 KiB) is reachable within a single receive window.
        YamuxConfig c = YamuxConfig.builder()
                .allocator(TestFrames.ALLOC)
                .maxStreamWindow(YamuxConstants.INITIAL_WINDOW)
                .build();
        YamuxSession s = new YamuxSession(c, out, ref::set);
        ByteBuf syn = TestFrames.header(FrameType.WINDOW_UPDATE, Flags.SYN, 1, SYN_DELTA);
        s.receive(syn);
        syn.release();
        YamuxStream st = ref.get();
        List<ByteBuf> delivered = new ArrayList<>();
        st.dataHandler(delivered::add);

        // First: a small amount consumed. pendingAck is still owed, so consumed()
        // flushes a WINDOW_UPDATE carrying ACK with the small delta.
        int small = 100 * 1024; // 100 KiB, below half-window (128 KiB)
        ByteBuf d1 = TestFrames.data(0, 1, new byte[small]);
        s.receive(d1);
        d1.release();
        st.consumed(small);
        assertTrue(out.frames.stream().anyMatch(r -> r.type == FrameType.WINDOW_UPDATE
                && Flags.has(r.flags, Flags.ACK) && r.length == small),
                "first consumed with pending ACK emits a WU carrying ACK");
        out.clear();

        // Now, with ACK already sent, feed just under then over the half-window
        // threshold; only crossing maxWindow/2 emits a WU (no ACK this time).
        int half = c.maxStreamWindow / 2; // 128 KiB
        int belowHalf = half - 1;
        ByteBuf d2 = TestFrames.data(0, 1, new byte[belowHalf]);
        s.receive(d2);
        d2.release();
        st.consumed(belowHalf);
        assertFalse(out.frames.stream().anyMatch(r -> r.type == FrameType.WINDOW_UPDATE),
                "below the half-window threshold, no WINDOW_UPDATE is emitted");

        ByteBuf d3 = TestFrames.data(0, 1, new byte[2]);
        s.receive(d3);
        d3.release();
        st.consumed(2); // cumulative now >= half
        assertTrue(out.frames.stream().anyMatch(r -> r.type == FrameType.WINDOW_UPDATE
                && r.length == belowHalf + 2),
                "crossing the half-window threshold emits a WINDOW_UPDATE with the accumulated delta");

        for (ByteBuf b : delivered) {
            b.release();
        }
    }

    @Test
    void finTeardownIsWindowUpdate() {
        CapturingOutbound out = new CapturingOutbound();
        AtomicReference<YamuxStream> ref = new AtomicReference<>();
        YamuxSession s = session(out, ref);
        YamuxStream st = open(s, ref, 1);
        boolean[] ended = {false};
        st.endHandler(() -> ended[0] = true);

        // Peer FIN via WINDOW_UPDATE(FIN).
        ByteBuf fin = TestFrames.header(FrameType.WINDOW_UPDATE, Flags.FIN, 1, 0);
        s.receive(fin);
        fin.release();
        assertTrue(ended[0]);

        out.clear();
        st.close();
        CapturingOutbound.Rec r = out.frames.stream()
                .filter(x -> Flags.has(x.flags, Flags.FIN)).findFirst().orElseThrow();
        assertEquals(FrameType.WINDOW_UPDATE, r.type, "FIN must be framed as WINDOW_UPDATE, not DATA");
        assertTrue(Flags.has(r.flags, Flags.FIN));
        assertEquals(0, r.length);
    }

    @Test
    void resetIsWindowUpdate() {
        CapturingOutbound out = new CapturingOutbound();
        AtomicReference<YamuxStream> ref = new AtomicReference<>();
        YamuxSession s = session(out, ref);
        YamuxStream st = open(s, ref, 1);
        out.clear();
        st.reset();
        CapturingOutbound.Rec r = out.frames.stream()
                .filter(x -> Flags.has(x.flags, Flags.RST)).findFirst().orElseThrow();
        assertEquals(FrameType.WINDOW_UPDATE, r.type, "RST must be framed as WINDOW_UPDATE, not DATA");
        assertTrue(Flags.has(r.flags, Flags.RST));
        assertEquals(0, r.length);
    }

    @Test
    void dataFinDeliversPayloadBeforeEnd() {
        CapturingOutbound out = new CapturingOutbound();
        AtomicReference<YamuxStream> ref = new AtomicReference<>();
        YamuxSession s = session(out, ref);
        YamuxStream st = open(s, ref, 1);

        List<String> order = new ArrayList<>();
        st.dataHandler(bb -> {
            order.add("data:" + bb.readableBytes());
            bb.release();
        });
        st.endHandler(() -> order.add("end"));

        ByteBuf d = TestFrames.data(Flags.FIN, 1, new byte[64]);
        s.receive(d);
        d.release();

        assertEquals(List.of("data:64", "end"), order, "payload must be delivered before endHandler");
    }

    @Test
    void frameSpanningChunks() {
        CapturingOutbound out = new CapturingOutbound();
        AtomicReference<YamuxStream> ref = new AtomicReference<>();
        YamuxSession s = session(out, ref);
        YamuxStream st = open(s, ref, 1);
        List<Integer> sizes = new ArrayList<>();
        st.dataHandler(bb -> {
            sizes.add(bb.readableBytes());
            bb.release();
        });

        // Build the exact wire bytes for a 100-byte DATA frame.
        ByteBuf frame = TestFrames.data(0, 1, new byte[100]);
        byte[] wire = new byte[frame.readableBytes()];
        frame.readBytes(wire);
        frame.release();

        // Split header/payload across two receive calls.
        ByteBuf part1 = Unpooled.buffer();
        part1.writeBytes(wire, 0, 6);
        s.receive(part1);
        part1.release();
        assertTrue(sizes.isEmpty());

        ByteBuf part2 = Unpooled.buffer();
        part2.writeBytes(wire, 6, wire.length - 6);
        s.receive(part2);
        part2.release();

        assertEquals(List.of(100), sizes, "spanning frame delivered once, intact");
    }

    @Test
    void accumulatorIsNotCompositeLeak() {
        CapturingOutbound out = new CapturingOutbound();
        AtomicReference<YamuxStream> ref = new AtomicReference<>();
        YamuxSession s = session(out, ref);
        YamuxStream st = open(s, ref, 1);
        st.dataHandler(ByteBuf::release);

        // Feed 1000 small caller-owned chunks; assert each can be released right after receive().
        for (int i = 0; i < 1000; i++) {
            ByteBuf chunk = TestFrames.data(0, 1, new byte[16]);
            s.receive(chunk);
            assertEquals(1, chunk.refCnt(), "session must not retain the caller's buffer");
            chunk.release();
        }
        assertFalse(s.isClosed());
    }

    @Test
    void keepaliveTimeoutClosesSession() {
        CapturingOutbound out = new CapturingOutbound();
        YamuxConfig c = YamuxConfig.builder()
                .allocator(TestFrames.ALLOC)
                .keepAliveTimeout(java.time.Duration.ofMillis(10))
                .build();
        YamuxSession s = new YamuxSession(c, out, st -> {});
        s.sendPing();
        assertFalse(s.isClosed());
        s.tick(System.currentTimeMillis() + 1000);
        assertTrue(s.isClosed(), "no pong within timeout must close the session");
    }
}
