package io.tunproto.yamux;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;

/**
 * A single yamux stream. Single-threaded by contract (same thread as its
 * {@link YamuxSession}). No locks.
 *
 * <p>FIN and RST are framed as WINDOW_UPDATE (type=1) with delta 0, matching
 * hashicorp yamux stream.go sendClose/forceClose (SPEC §4, Blocker 1).</p>
 */
public final class YamuxStream {

    private static final int HIGH_WATER = 1 << 20; // 1 MiB
    private static final int LOW_WATER = 256 * 1024; // 256 KiB

    private final long id;
    private final YamuxConfig config;
    private final YamuxSession session;
    private final ByteBufAllocator alloc;

    // Flow control windows.
    int sendWindow; // how many bytes we may send to the peer
    int recvWindow; // how many bytes the peer may send us before overrun
    private int recvConsumed; // bytes consumed by the app awaiting replenish
    boolean pendingAck; // owe ACK; piggyback on first outbound frame

    // Send queue (DOWN direction: target -> peer).
    private final Deque<ByteBuf> sendQueue = new ArrayDeque<>();
    private long sendQueueBytes = 0;

    // Half-close state.
    private boolean localClosed = false;
    private boolean remoteClosed = false;
    private boolean closedFired = false;

    // Handlers.
    private Consumer<ByteBuf> dataHandler;
    private Runnable endHandler;
    private Runnable closeHandler;
    private Runnable drainHandler;

    YamuxStream(long id, YamuxConfig config, YamuxSession session) {
        this.id = id;
        this.config = config;
        this.session = session;
        this.alloc = config.allocator;
    }

    public long id() {
        return id;
    }

    // ---- receiver side (peer -> target, the UP direction), driven by the session ----

    public void dataHandler(Consumer<ByteBuf> onData) {
        this.dataHandler = onData;
    }

    public void endHandler(Runnable onEnd) {
        this.endHandler = onEnd;
    }

    public void closeHandler(Runnable onClose) {
        this.closeHandler = onClose;
        if (closedFired && onClose != null) {
            onClose.run();
        }
    }

    public void drainHandler(Runnable onDrain) {
        this.drainHandler = onDrain;
    }

    /** Deliver an inbound payload slice to the app. Ownership transfers to handler. */
    void deliverData(ByteBuf payload) {
        if (dataHandler != null) {
            dataHandler.accept(payload);
        } else {
            payload.release();
        }
    }

    void markRemoteFin() {
        if (remoteClosed) {
            return;
        }
        remoteClosed = true;
        if (endHandler != null) {
            endHandler.run();
        }
        maybeFireClose();
    }

    void markRemoteRst() {
        if (remoteClosed && localClosed) {
            return;
        }
        remoteClosed = true;
        localClosed = true;
        if (endHandler != null) {
            endHandler.run();
        }
        maybeFireClose();
    }

    /**
     * RECEIVER replenish (lesson #2). Call AFTER the target has accepted {@code n}
     * bytes, so a slow target throttles our WINDOW_UPDATE emission (OOM guard).
     */
    public void consumed(int n) {
        if (n <= 0) {
            return;
        }
        recvConsumed += n;
        int delta = recvConsumed;
        if (delta > 0 && (delta >= config.maxStreamWindow / 2 || pendingAck)) {
            int fl = 0;
            if (pendingAck) {
                fl |= Flags.ACK;
                pendingAck = false;
            }
            recvWindow += delta;
            recvConsumed = 0;
            session.emit(FrameCodec.encodeHeader(alloc, FrameType.WINDOW_UPDATE, fl, id, delta));
        }
    }

    // ---- sender side (target -> peer, the DOWN direction) ----

    /**
     * Enqueue data to send to the peer (honoring the send window). Ownership of
     * {@code data} transfers to the stream; it is released when fully sent.
     * Returns the number of bytes accepted into a frame right now.
     */
    public int write(ByteBuf data) {
        if (localClosed) {
            data.release();
            return 0;
        }
        if (data.readableBytes() == 0) {
            data.release();
            return 0;
        }
        sendQueueBytes += data.readableBytes();
        sendQueue.addLast(data);
        return flush();
    }

    public boolean writeQueueFull() {
        return sendQueueBytes > HIGH_WATER;
    }

    /**
     * Push queued bytes to the peer up to the send window. Called on write() and
     * when a WINDOW_UPDATE arrives. Never dequeues while the session outbound is
     * paused (§4.9).
     */
    int flush() {
        if (session.isOutboundPaused()) {
            return 0;
        }
        int sent = 0;
        while (!sendQueue.isEmpty() && sendWindow > 0) {
            ByteBuf head = sendQueue.peekFirst();
            int n = Math.min(Math.min(sendWindow, head.readableBytes()), YamuxConstants.MAX_FRAME_PAYLOAD);
            ByteBuf chunk = head.readRetainedSlice(n);
            if (!head.isReadable()) {
                sendQueue.pollFirst();
                head.release();
            }
            int fl = 0;
            if (pendingAck) {
                fl |= Flags.ACK;
                pendingAck = false;
            }
            session.emit(FrameCodec.encodeData(alloc, fl, id, chunk));
            sendWindow -= n;
            sendQueueBytes -= n;
            sent += n;
        }
        if (sendQueueBytes <= LOW_WATER && drainHandler != null) {
            drainHandler.run();
        }
        return sent;
    }

    /** Send FIN (WINDOW_UPDATE flags=FIN delta=0). Idempotent. */
    public void close() {
        if (localClosed) {
            return;
        }
        localClosed = true;
        int fl = Flags.FIN;
        if (pendingAck) {
            fl |= Flags.ACK;
            pendingAck = false;
        }
        session.emit(FrameCodec.encodeHeader(alloc, FrameType.WINDOW_UPDATE, fl, id, 0));
        maybeFireClose();
    }

    /** Send RST (WINDOW_UPDATE flags=RST delta=0). Marks closed both ways. */
    public void reset() {
        if (localClosed && remoteClosed) {
            return;
        }
        boolean wasLocalClosed = localClosed;
        localClosed = true;
        remoteClosed = true;
        if (!wasLocalClosed) {
            int fl = Flags.RST;
            if (pendingAck) {
                fl |= Flags.ACK;
                pendingAck = false;
            }
            session.emit(FrameCodec.encodeHeader(alloc, FrameType.WINDOW_UPDATE, fl, id, 0));
        }
        maybeFireClose();
    }

    /** Hard local teardown: drop queued send buffers, mark closed. Used by session.close(). */
    void forceClose() {
        localClosed = true;
        remoteClosed = true;
        ByteBuf b;
        while ((b = sendQueue.pollFirst()) != null) {
            b.release();
        }
        sendQueueBytes = 0;
        maybeFireClose();
    }

    private void maybeFireClose() {
        if (closedFired) {
            return;
        }
        if (localClosed && remoteClosed) {
            closedFired = true;
            session.onStreamClosed(this);
            if (closeHandler != null) {
                closeHandler.run();
            }
        }
    }

    public boolean isRemoteClosed() {
        return remoteClosed;
    }

    public boolean isLocalClosed() {
        return localClosed;
    }

    boolean hasPendingSend() {
        return !sendQueue.isEmpty();
    }
}
