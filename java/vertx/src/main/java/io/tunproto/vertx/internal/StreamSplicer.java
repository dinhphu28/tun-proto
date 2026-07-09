package io.tunproto.vertx.internal;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.tunproto.vertx.TargetPolicy;
import io.tunproto.vertx.events.StreamErrorEvent;
import io.tunproto.vertx.events.StreamEvent;
import io.tunproto.yamux.OpenRequest;
import io.tunproto.yamux.OpenRequestReader;
import io.tunproto.yamux.ProtocolException;
import io.tunproto.yamux.YamuxStream;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Splices one accepted yamux stream to a freshly dialed target TCP socket, with
 * full bidirectional backpressure and correct WINDOW_UPDATE replenish accounting.
 *
 * <p>Lifecycle:</p>
 * <ol>
 *   <li><b>Phase 1 (reading OpenRequest):</b> feed inbound stream DATA to an
 *       {@link OpenRequestReader}. On completion, apply the {@link TargetPolicy}
 *       and dial via the shared async {@code NetClient}. DATA that arrives while
 *       dialing is queued (byte count tracked for replenish).</li>
 *   <li><b>Phase 2 (spliced):</b>
 *     <ul>
 *       <li>UP (peer -&gt; target): write payloads to the socket; call
 *           {@code stream.consumed(n)} ONLY after the socket accepts the bytes
 *           (write completion), throttling replenish to target drain speed (OOM
 *           guard). Replenishes the OpenRequest {@code consumedBytes} too.</li>
 *       <li>DOWN (target -&gt; peer): {@code stream.write(buf)}; pause the socket
 *           when {@code stream.writeQueueFull()}, resume on the stream's
 *           drainHandler.</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <p>All callbacks run on the session's event loop (the NetClient inherits the
 * calling context), so no locks are needed.</p>
 */
final class StreamSplicer {

    private final String sessionId;
    private final YamuxStream stream;
    private final NetClient netClient;
    private final TargetPolicy policy;
    private final Handler<StreamEvent> onStream;
    private final Handler<StreamEvent> onStreamClose;
    private final Handler<StreamErrorEvent> onStreamError;
    private final Runnable onTerminal; // notify owner (session) that this stream ended

    private OpenRequestReader reader;
    private NetSocket sock;

    private boolean dialing = false;
    private boolean spliced = false;
    private boolean terminated = false;

    // OpenRequest bytes to replenish once the leftover is accepted by the target.
    private int openRequestConsumed = 0;

    // DATA that arrived after the OpenRequest but before the socket connected.
    private final Deque<ByteBuf> pending = new ArrayDeque<>();

    private String targetHost;
    private int targetPort;

    StreamSplicer(String sessionId,
                  YamuxStream stream,
                  NetClient netClient,
                  TargetPolicy policy,
                  Handler<StreamEvent> onStream,
                  Handler<StreamEvent> onStreamClose,
                  Handler<StreamErrorEvent> onStreamError,
                  Runnable onTerminal) {
        this.sessionId = sessionId;
        this.stream = stream;
        this.netClient = netClient;
        this.policy = policy;
        this.onStream = onStream;
        this.onStreamClose = onStreamClose;
        this.onStreamError = onStreamError;
        this.onTerminal = onTerminal;
    }

    /** Wire the stream handlers. Called once, on the event loop. */
    void start() {
        this.reader = new OpenRequestReader();
        stream.dataHandler(this::onInboundData);
        stream.endHandler(this::onPeerFin);
        stream.closeHandler(this::onStreamFullyClosed);
    }

    // ---- Phase 1: read OpenRequest, then dial ----

    private void onInboundData(ByteBuf payload) {
        if (terminated) {
            payload.release();
            return;
        }
        if (spliced) {
            // Phase 2 UP: write to socket, replenish on accept. Copy into an owned
            // buffer: the incoming slice is a view into the session accumulator,
            // which mutates as later frames arrive before our async write flushes.
            forwardToTarget(copyAndRelease(payload));
            return;
        }
        if (dialing) {
            // Queue until the socket is connected. Copy off the session accumulator
            // slice so later inbound frames cannot corrupt the buffered bytes.
            pending.addLast(copyAndRelease(payload));
            return;
        }
        // Still reading the OpenRequest.
        OpenRequestReader.Result result;
        try {
            result = reader.offer(payload); // ownership transfers; released inside
        } catch (ProtocolException e) {
            fail(null, e);
            return;
        }
        if (result == null) {
            return; // need more bytes
        }

        OpenRequest req = result.request;
        try {
            this.targetHost = req.host();
            this.targetPort = req.port();
        } catch (ProtocolException e) {
            result.leftover.release();
            fail(null, e);
            return;
        }

        if (policy != null && !policy.allow(targetHost, targetPort)) {
            result.leftover.release();
            fail(targetHost + ":" + targetPort,
                    new SecurityException("egress denied by TargetPolicy: " + targetHost + ":" + targetPort));
            return;
        }

        // These OpenRequest bytes already debited recvWindow; replenish once the
        // leftover is accepted by the target (or immediately if no leftover).
        this.openRequestConsumed = result.consumedBytes;

        // Queue the leftover as the first payload to forward.
        if (result.leftover.readableBytes() > 0) {
            pending.addLast(result.leftover);
        } else {
            result.leftover.release();
        }

        if (onStream != null) {
            onStream.handle(new StreamEvent(sessionId, stream.id(), targetHost, targetPort));
        }

        dialing = true;
        netClient.connect(targetPort, targetHost).onComplete(ar -> {
            if (ar.succeeded()) {
                onConnected(ar.result());
            } else {
                onDialFailed(ar.cause());
            }
        });
    }

    private void onDialFailed(Throwable cause) {
        drainPending();
        fail(targetHost + ":" + targetPort, cause);
    }

    // ---- Phase 2: spliced ----

    private void onConnected(NetSocket socket) {
        if (terminated) {
            socket.close();
            drainPending();
            return;
        }
        this.sock = socket;
        this.dialing = false;
        this.spliced = true;

        // DOWN direction: target -> peer.
        socket.handler(this::onTargetData);
        socket.endHandler(v -> onTargetEof());
        socket.closeHandler(v -> onTargetClosed());
        socket.exceptionHandler(t -> onTargetClosed());

        // DOWN backpressure: when the yamux send queue is full, pause the socket;
        // resume when it drains below the low-water mark.
        stream.drainHandler(() -> {
            if (sock != null) {
                sock.resume();
            }
        });

        // Replenish the OpenRequest header bytes as soon as we are connected: the
        // reader already handed us the leftover which we forward below; the header
        // (4 + jsonLen) has no target-side backpressure so it is safe to replenish.
        // We fold it into the first forwarded chunk's accounting via forwardToTarget.

        // Flush any queued payloads (leftover + DATA that arrived while dialing).
        flushPending();
    }

    /** Forward one inbound payload to the target socket; replenish on accept. */
    private void forwardToTarget(ByteBuf payload) {
        int n = payload.readableBytes();
        if (n == 0) {
            payload.release();
            replenishOpenRequest();
            return;
        }
        Buffer buf = BufferBridge.wrap(payload); // wraps; write retains lifecycle
        sock.write(buf).onComplete(ar -> {
            payload.release();
            if (terminated) {
                return;
            }
            if (ar.succeeded()) {
                // Replenish only AFTER the socket accepted the bytes (OOM guard).
                stream.consumed(n);
                replenishOpenRequest();
            } else {
                onTargetClosed();
            }
        });
        // If the socket write queue is full, backpressure is applied implicitly:
        // we delay stream.consumed() until the write completes, so the peer's send
        // window drains and it stops sending. No per-stream inbound pause API exists.
    }

    private void flushPending() {
        // Forward queued payloads in order. Each goes through forwardToTarget so
        // replenish accounting is honored.
        ByteBuf p;
        while ((p = pending.pollFirst()) != null) {
            forwardToTarget(p);
        }
        if (openRequestConsumed > 0 && pending.isEmpty()) {
            // No payloads at all (empty leftover, no queued DATA): replenish now.
            replenishOpenRequest();
        }
    }

    /** Replenish the OpenRequest header/leftover bytes exactly once. */
    private void replenishOpenRequest() {
        if (openRequestConsumed > 0) {
            int c = openRequestConsumed;
            openRequestConsumed = 0;
            stream.consumed(c);
        }
    }

    private void onTargetData(Buffer buf) {
        if (terminated) {
            return;
        }
        // buf.getByteBuf() returns a slice sharing Vert.x's refcount; copy into a
        // buffer we own so stream.write() can release it safely when fully sent.
        ByteBuf src = buf.getByteBuf();
        ByteBuf owned = Unpooled.buffer(src.readableBytes());
        owned.writeBytes(src, src.readerIndex(), src.readableBytes());
        stream.write(owned); // ownership transfers
        if (stream.writeQueueFull() && sock != null) {
            sock.pause();
        }
    }

    // ---- half-close / teardown ----

    /**
     * Peer sent FIN (it is done sending UP). Vert.x {@code NetSocket} does not
     * support TCP half-close ({@code end()} fully closes the connection and would
     * discard any in-flight target reply), so we do NOT close the target here.
     * The DOWN direction keeps flowing until the target itself reaches EOF, at
     * which point we send FIN to the peer. Full teardown happens on target EOF /
     * close or on RST.
     */
    private void onPeerFin() {
        // Intentionally no socket action: keep the target open for its response.
    }

    /** Target reached EOF: send FIN to the peer. */
    private void onTargetEof() {
        if (terminated) {
            return;
        }
        stream.close(); // FIN
    }

    /** Target socket fully closed: reset the stream (RST). Guarded. */
    private void onTargetClosed() {
        if (terminated) {
            return;
        }
        stream.reset();
    }

    /** Stream fully closed (both ways): close the target socket. Guarded. */
    private void onStreamFullyClosed() {
        terminate();
    }

    /** Reject/fail path: RST the stream and emit an error event. */
    private void fail(String targetAddress, Throwable cause) {
        if (onStreamError != null) {
            onStreamError.handle(new StreamErrorEvent(sessionId, stream.id(), targetAddress, cause));
        }
        stream.reset(); // triggers closeHandler -> terminate()
    }

    private void drainPending() {
        ByteBuf p;
        while ((p = pending.pollFirst()) != null) {
            p.release();
        }
    }

    /**
     * Copy an inbound payload (a slice into the session's shared, mutating
     * accumulator) into an independently owned heap buffer, releasing the slice.
     * Required whenever the payload outlives the synchronous receive call (queued
     * while dialing, or held across an async socket write).
     */
    private static ByteBuf copyAndRelease(ByteBuf slice) {
        try {
            ByteBuf owned = Unpooled.buffer(slice.readableBytes());
            owned.writeBytes(slice, slice.readerIndex(), slice.readableBytes());
            return owned;
        } finally {
            slice.release();
        }
    }

    /** Idempotent terminal cleanup: release queued buffers, close socket, notify owner. */
    private void terminate() {
        if (terminated) {
            return;
        }
        terminated = true;
        drainPending();
        if (reader != null) {
            reader.release(); // idempotent
        }
        if (sock != null) {
            sock.close();
        }
        if (onStreamClose != null) {
            onStreamClose.handle(new StreamEvent(sessionId, stream.id(), targetHost, targetPort));
        }
        if (onTerminal != null) {
            onTerminal.run();
        }
    }
}
