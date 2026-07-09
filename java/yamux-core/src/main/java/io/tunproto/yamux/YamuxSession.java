package io.tunproto.yamux;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Sans-IO yamux SERVER session. Single-threaded by contract: all of
 * receive/write/consumed/tick/sendPing/close for one session run on the same
 * thread (the connection's event loop). No locks.
 *
 * <p>The server is the yamux SERVER and never opens streams. Clients use ODD
 * ids; even inbound SYN is a protocol error.</p>
 */
public final class YamuxSession {

    private static final int CONTROL_QUEUE_CAP = 4096;

    private final YamuxConfig config;
    private final OutboundHandler out;
    private final StreamHandler onStream;
    private final ByteBufAllocator alloc;

    private final Map<Long, YamuxStream> streams = new HashMap<>();
    private long lastAcceptedOdd = -1;
    private int active = 0;

    // Incremental parse accumulator (single growable heap buffer; we COPY into it).
    private final ByteBuf accum;

    // Keepalive.
    private long lastPongMillis = -1;
    private boolean pingSent = false;
    private long pingOpaque = 1;

    // Outbound pause (transport backpressure): buffer CONTROL frames only.
    private boolean outboundPaused = false;
    private final Deque<ByteBuf> controlQueue = new ArrayDeque<>();

    private boolean goingAway = false;
    private boolean closed = false;

    public YamuxSession(YamuxConfig config, OutboundHandler out, StreamHandler onStream) {
        this.config = config;
        this.out = out;
        this.onStream = onStream;
        this.alloc = config.allocator;
        this.accum = config.allocator.heapBuffer(1024);
    }

    // ---- outbound ----

    void emit(ByteBuf frame) {
        if (closed) {
            frame.release();
            return;
        }
        if (outboundPaused) {
            if (controlQueue.size() >= CONTROL_QUEUE_CAP) {
                frame.release();
                goAway(YamuxConstants.GOAWAY_INTERNAL);
                close();
                return;
            }
            controlQueue.addLast(frame);
            return;
        }
        out.onFrame(frame);
    }

    boolean isOutboundPaused() {
        return outboundPaused;
    }

    /** Transport backpressure engaged: buffer only CONTROL frames; DATA stays in stream queues. */
    public void pauseOutbound() {
        outboundPaused = true;
    }

    /** Transport drained: flush buffered control frames, then resume stream flushing. */
    public void resumeOutbound() {
        outboundPaused = false;
        ByteBuf f;
        while ((f = controlQueue.pollFirst()) != null) {
            if (closed) {
                f.release();
            } else {
                out.onFrame(f);
            }
        }
        // Resume stream data sends.
        for (YamuxStream s : streams.values().toArray(new YamuxStream[0])) {
            if (s.hasPendingSend()) {
                s.flush();
            }
        }
    }

    // ---- inbound ----

    /**
     * Feed inbound transport bytes. Caller RETAINS ownership of {@code buf}; the
     * session copies what it needs and never releases {@code buf}.
     */
    public void receive(ByteBuf buf) {
        if (closed) {
            return;
        }
        try {
            accum.writeBytes(buf, buf.readerIndex(), buf.readableBytes());
            parseLoop();
            accum.discardSomeReadBytes();
            // Bound the accumulator: a valid frame is at most HEADER + maxWindow.
            if (accum.readableBytes() > YamuxConstants.HEADER_LEN + (long) config.maxStreamWindow) {
                throw new ProtocolException("accumulator overflow", YamuxConstants.GOAWAY_PROTO);
            }
        } catch (ProtocolException e) {
            goAway(e.goAwayCode);
            close();
        }
    }

    private void parseLoop() {
        while (true) {
            int ri = accum.readerIndex();
            if (accum.readableBytes() < YamuxConstants.HEADER_LEN) {
                return;
            }
            if (FrameCodec.version(accum, ri) != YamuxConstants.VERSION) {
                throw new ProtocolException("bad version", YamuxConstants.GOAWAY_PROTO);
            }
            FrameType type = FrameType.from(FrameCodec.type(accum, ri));
            int flags = FrameCodec.flags(accum, ri);
            long sid = FrameCodec.streamId(accum, ri);
            long len = FrameCodec.length(accum, ri);

            if (type == FrameType.DATA) {
                if (len > config.maxStreamWindow) {
                    throw new ProtocolException("data length exceeds max window", YamuxConstants.GOAWAY_PROTO);
                }
                if (accum.readableBytes() < YamuxConstants.HEADER_LEN + len) {
                    return; // wait for full payload
                }
                ByteBuf payload = accum.retainedSlice(ri + YamuxConstants.HEADER_LEN, (int) len);
                accum.skipBytes(YamuxConstants.HEADER_LEN + (int) len);
                handleData(sid, flags, payload);
            } else {
                accum.skipBytes(YamuxConstants.HEADER_LEN);
                switch (type) {
                    case WINDOW_UPDATE -> handleWindowUpdate(sid, flags, len);
                    case PING -> handlePing(flags, len);
                    case GO_AWAY -> handleGoAway(len);
                    default -> throw new ProtocolException("unexpected type", YamuxConstants.GOAWAY_PROTO);
                }
            }
        }
    }

    private void handleData(long sid, int flags, ByteBuf payload) {
        if (sid == YamuxConstants.SESSION_STREAM_ID) {
            payload.release();
            throw new ProtocolException("data on session stream", YamuxConstants.GOAWAY_PROTO);
        }
        YamuxStream stream = ensureAcceptedStream(sid, flags);
        if (stream == null) {
            payload.release();
            return;
        }
        if (Flags.has(flags, Flags.RST)) {
            payload.release();
            stream.markRemoteRst();
            return;
        }
        int n = payload.readableBytes();
        stream.recvWindow -= n;
        if (stream.recvWindow < 0) {
            payload.release();
            throw new ProtocolException("peer overran receive window", YamuxConstants.GOAWAY_PROTO);
        }
        if (n > 0) {
            stream.deliverData(payload); // ownership -> handler
        } else {
            payload.release();
        }
        if (Flags.has(flags, Flags.FIN)) {
            stream.markRemoteFin(); // AFTER delivering payload (Blocker 2)
        }
    }

    private void handleWindowUpdate(long sid, int flags, long delta) {
        if (sid == YamuxConstants.SESSION_STREAM_ID) {
            return; // session-level WU unused
        }
        YamuxStream stream;
        if (Flags.has(flags, Flags.SYN)) {
            stream = ensureAcceptedStream(sid, flags); // CREATE now
            if (stream == null) {
                return; // rejected -> RST already emitted
            }
            stream.sendWindow += (int) delta; // === LESSON #1: APPLY SYN DELTA ===
        } else {
            stream = streams.get(sid);
            if (stream == null) {
                return;
            }
            stream.sendWindow += (int) delta;
            stream.flush(); // WU may unblock buffered writes
        }
        // FIN/RST may ride a WINDOW_UPDATE (hashicorp). Process AFTER delta.
        if (Flags.has(flags, Flags.RST)) {
            stream.markRemoteRst();
            return;
        }
        if (Flags.has(flags, Flags.FIN)) {
            stream.markRemoteFin();
        }
    }

    private void handlePing(int flags, long opaque) {
        if (Flags.has(flags, Flags.SYN)) {
            emit(FrameCodec.encodeHeader(alloc, FrameType.PING, Flags.ACK, YamuxConstants.SESSION_STREAM_ID, opaque));
        } else if (Flags.has(flags, Flags.ACK)) {
            lastPongMillis = System.currentTimeMillis();
        }
    }

    private void handleGoAway(long code) {
        // Peer is going away; stop accepting new streams. Existing streams continue.
        goingAway = true;
    }

    private YamuxStream ensureAcceptedStream(long sid, int flags) {
        YamuxStream existing = streams.get(sid);
        if (existing != null) {
            return existing;
        }
        if (!Flags.has(flags, Flags.SYN)) {
            throw new ProtocolException("new stream id without SYN: " + sid, YamuxConstants.GOAWAY_PROTO);
        }
        if ((sid & 1L) == 0L) {
            throw new ProtocolException("even inbound SYN: " + sid, YamuxConstants.GOAWAY_PROTO);
        }
        // NOTE: we intentionally do NOT reject an odd id that is <= the highest id
        // accepted so far. Real hashicorp-yamux clients (the Go tunnel client) assign
        // stream ids from a monotonic counter under lock but emit each SYN lazily on
        // the stream's first Write, from that caller's goroutine. Under concurrent
        // OpenStream the SYN FRAMES therefore reach the wire out of id order (e.g.
        // SYN(3) before SYN(1)), even though ids are assigned monotonically. A strict
        // "sid must exceed the highest accepted id" check rejected those valid
        // concurrent opens and broke interop (SPEC conformance requires N>=8
        // concurrent streams). The streams-map lookup above already returns the
        // existing stream for a live duplicate, so any not-currently-tracked odd id is
        // a legitimate new stream. Ids stay odd-only and are never reused per session.
        if (goingAway) {
            emitReset(sid);
            return null;
        }
        if (config.maxConcurrentStreams > 0 && active >= config.maxConcurrentStreams) {
            emitReset(sid);
            return null;
        }
        YamuxStream s = new YamuxStream(sid, config, this);
        s.recvWindow = config.initialWindow;
        s.sendWindow = config.initialWindow; // SYN delta bumps sendWindow next
        s.pendingAck = true;
        streams.put(sid, s);
        if (sid > lastAcceptedOdd) {
            lastAcceptedOdd = sid;
        }
        active++;
        onStream.onStream(s);
        return s;
    }

    private void emitReset(long sid) {
        emit(FrameCodec.encodeHeader(alloc, FrameType.WINDOW_UPDATE, Flags.RST, sid, 0));
    }

    void onStreamClosed(YamuxStream s) {
        if (streams.remove(s.id()) != null) {
            active--;
        }
    }

    // ---- keepalive ----

    public void sendPing() {
        if (closed) {
            return;
        }
        long opaque = pingOpaque++;
        pingSent = true;
        if (lastPongMillis < 0) {
            lastPongMillis = System.currentTimeMillis();
        }
        emit(FrameCodec.encodeHeader(alloc, FrameType.PING, Flags.SYN, YamuxConstants.SESSION_STREAM_ID, opaque));
    }

    /** Enforce keepalive timeout: teardown if no pong within keepAliveTimeout after a ping. */
    public void tick(long nowMillis) {
        if (closed || !pingSent) {
            return;
        }
        long timeoutMs = config.keepAliveTimeout.toMillis();
        if (lastPongMillis >= 0 && nowMillis - lastPongMillis > timeoutMs) {
            close();
        }
    }

    // ---- teardown ----

    public void goAway(long code) {
        if (closed) {
            return;
        }
        goingAway = true;
        if (!outboundPaused) {
            out.onFrame(FrameCodec.encodeHeader(alloc, FrameType.GO_AWAY, 0, YamuxConstants.SESSION_STREAM_ID, code));
        }
    }

    /** Hard local teardown; reset all streams; free buffers. Idempotent. */
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        for (YamuxStream s : streams.values().toArray(new YamuxStream[0])) {
            s.forceClose();
        }
        streams.clear();
        active = 0;
        ByteBuf f;
        while ((f = controlQueue.pollFirst()) != null) {
            f.release();
        }
        if (accum.refCnt() > 0) {
            accum.release();
        }
    }

    public boolean isClosed() {
        return closed;
    }

    public int activeStreamCount() {
        return active;
    }
}
