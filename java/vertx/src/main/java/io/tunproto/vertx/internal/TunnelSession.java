package io.tunproto.vertx.internal;

import io.netty.buffer.ByteBuf;
import io.tunproto.vertx.TunnelOptions;
import io.tunproto.vertx.events.StreamErrorEvent;
import io.tunproto.vertx.events.StreamEvent;
import io.tunproto.yamux.OutboundHandler;
import io.tunproto.yamux.StreamHandler;
import io.tunproto.yamux.YamuxConfig;
import io.tunproto.yamux.YamuxConstants;
import io.tunproto.yamux.YamuxSession;
import io.tunproto.yamux.YamuxStream;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.net.NetClient;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * One tunnel session bound to a single WebSocket connection, pinned to that
 * connection's event loop. Owns exactly one {@link YamuxSession}.
 *
 * <p>Wiring (all on the WS event loop):</p>
 * <ul>
 *   <li>Outbound yamux frames -&gt; WS binary writes; release the ByteBuf AFTER
 *       the write completes.</li>
 *   <li>WS binary frames -&gt; {@code session.receive} (all bytes; boundaries
 *       are not frame boundaries).</li>
 *   <li>Transport backpressure: {@code ws.writeQueueFull()} -&gt;
 *       {@code pauseOutbound}; {@code ws.drainHandler} -&gt; {@code resumeOutbound}.</li>
 *   <li>New streams -&gt; {@link StreamSplicer}.</li>
 *   <li>Keepalive: periodic {@code sendPing()} + {@code tick(now)}.</li>
 * </ul>
 */
final class TunnelSession {

    private final Vertx vertx;
    private final ServerWebSocket ws;
    private final TunnelOptions options;
    private final NetClient netClient;
    private final String sessionId;

    private final Handler<StreamEvent> onStream;
    private final Handler<StreamEvent> onStreamClose;
    private final Handler<StreamErrorEvent> onStreamError;

    // Global concurrent proxied streams limit (shared across the whole server).
    private final int maxStreams;
    private final AtomicInteger globalStreamCount;

    private YamuxSession session;
    private long keepAliveTimer = -1;
    private boolean closed = false;

    TunnelSession(Vertx vertx,
                  ServerWebSocket ws,
                  TunnelOptions options,
                  NetClient netClient,
                  String sessionId,
                  int maxStreams,
                  AtomicInteger globalStreamCount,
                  Handler<StreamEvent> onStream,
                  Handler<StreamEvent> onStreamClose,
                  Handler<StreamErrorEvent> onStreamError) {
        this.vertx = vertx;
        this.ws = ws;
        this.options = options;
        this.netClient = netClient;
        this.sessionId = sessionId;
        this.maxStreams = maxStreams;
        this.globalStreamCount = globalStreamCount;
        this.onStream = onStream;
        this.onStreamClose = onStreamClose;
        this.onStreamError = onStreamError;
    }

    String id() {
        return sessionId;
    }

    int activeStreams() {
        return session == null ? 0 : session.activeStreamCount();
    }

    /** Build the YamuxSession and wire all transport handlers. */
    void start() {
        YamuxConfig config = YamuxConfig.builder()
                .maxStreamWindow(options.getMaxStreamWindow())
                .maxConcurrentStreams(options.getMaxConcurrentStreamsPerSession())
                .keepAliveInterval(options.getKeepAliveInterval())
                .keepAliveTimeout(options.getKeepAliveTimeout())
                .build();

        OutboundHandler out = frame -> writeFrame(frame);
        StreamHandler streams = this::onNewStream;
        this.session = new YamuxSession(config, out, streams);

        // WS binary -> session.receive (Vert.x owns the inbound Buffer; do not release).
        ws.binaryMessageHandler(buf -> {
            if (closed) {
                return;
            }
            ByteBuf bb = buf.getByteBuf();
            session.receive(bb);
        });

        // Transport backpressure: WS drained -> resume outbound stream flushing.
        ws.drainHandler(v -> {
            if (!closed && session != null) {
                session.resumeOutbound();
            }
        });

        ws.closeHandler(v -> teardown());
        ws.exceptionHandler(t -> {
            if (!closed && session != null) {
                session.goAway(YamuxConstants.GOAWAY_INTERNAL);
            }
            teardown();
        });

        // Keepalive: ping + timeout enforcement.
        long intervalMs = Math.max(1, options.getKeepAliveInterval().toMillis());
        this.keepAliveTimer = vertx.setPeriodic(intervalMs, id -> {
            if (closed || session == null) {
                return;
            }
            session.sendPing();
            session.tick(System.currentTimeMillis());
            if (session.isClosed()) {
                closeWs();
            }
        });
    }

    /** Outbound frame: wrap zero-copy, write, release AFTER the write completes. */
    private void writeFrame(ByteBuf frame) {
        if (closed) {
            frame.release();
            return;
        }
        Buffer buf = BufferBridge.wrap(frame);
        ws.writeBinaryMessage(buf).onComplete(ar -> frame.release());
        // Engage transport backpressure if the WS write queue is now full.
        if (ws.writeQueueFull() && session != null) {
            session.pauseOutbound();
        }
    }

    private void onNewStream(YamuxStream stream) {
        // Global concurrent-streams cap: RST if over the limit.
        if (maxStreams > 0) {
            int now = globalStreamCount.incrementAndGet();
            if (now > maxStreams) {
                globalStreamCount.decrementAndGet();
                if (onStreamError != null) {
                    onStreamError.handle(new StreamErrorEvent(sessionId, stream.id(), null,
                            new IllegalStateException("global maxStreams exceeded: " + maxStreams)));
                }
                stream.reset();
                return;
            }
        }

        Runnable onTerminal = () -> {
            if (maxStreams > 0) {
                globalStreamCount.decrementAndGet();
            }
        };

        StreamSplicer splicer = new StreamSplicer(
                sessionId, stream, netClient, options.getAllowTarget(),
                onStream, onStreamClose, onStreamError, onTerminal);
        splicer.start();
    }

    /** Close the WS if still open (which will fire our closeHandler -> teardown). */
    private void closeWs() {
        try {
            ws.close();
        } catch (Exception ignore) {
            teardown();
        }
    }

    /** Idempotent session teardown. */
    void teardown() {
        if (closed) {
            return;
        }
        closed = true;
        if (keepAliveTimer >= 0) {
            vertx.cancelTimer(keepAliveTimer);
            keepAliveTimer = -1;
        }
        if (session != null) {
            session.close(); // resets all streams -> their closeHandlers -> sockets close
        }
    }

    /** Graceful shutdown from server.close(): GO_AWAY then teardown. */
    void shutdown() {
        if (closed) {
            return;
        }
        if (session != null) {
            session.goAway(YamuxConstants.GOAWAY_NORMAL);
        }
        teardown();
        closeWs();
    }
}
