package io.tunproto.vertx;

import io.tunproto.vertx.events.SessionEvent;
import io.tunproto.vertx.events.StreamErrorEvent;
import io.tunproto.vertx.events.StreamEvent;
import io.tunproto.vertx.internal.TunnelServerImpl;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;

/**
 * Embeddable tun-proto reverse-TCP tunnel SERVER for Vert.x. Wraps the sans-IO
 * {@code io.tunproto.yamux} engine so a developer never touches yamux,
 * WebSocket upgrades, or OpenRequest framing.
 *
 * <p>Three ways to stand it up:</p>
 * <pre>{@code
 * // 1. Own an HttpServer:
 * TunnelServer.create(vertx, new TunnelOptions().addApiKey("secret"))
 *     .listen(8080)
 *     .onSuccess(s -> log.info("tunnel on ws://:8080" + s.path()));
 *
 * // 2. Mount onto an existing vertx-web Router (shares the app's HttpServer/port):
 * TunnelServer tunnel = TunnelServer.create(vertx, options);
 * tunnel.mount(router);
 *
 * // 3. Mount onto an existing HttpServer (no vertx-web):
 * tunnel.mount(httpServer);
 * }</pre>
 *
 * <p>Fully non-blocking: one {@code YamuxSession} per WebSocket, pinned to that
 * connection's event loop.</p>
 */
public interface TunnelServer {

    /**
     * Create a tunnel server. Validates options (exactly one auth mode), builds
     * the shared yamux config, creates one shared async {@code NetClient}. Does
     * NOT bind.
     *
     * @throws IllegalArgumentException if options are invalid (e.g. no auth mode,
     *         or more than one auth mode configured).
     */
    static TunnelServer create(Vertx vertx, TunnelOptions options) {
        return TunnelServerImpl.create(vertx, options);
    }

    /** Create and own an {@code HttpServer}, install the WS handler, and bind. */
    Future<TunnelServer> listen(int port);

    /** Create and own an {@code HttpServer}, install the WS handler, and bind to host. */
    Future<TunnelServer> listen(int port, String host);

    /**
     * Mount onto an existing vertx-web {@link Router}: registers a route at
     * {@code options.getPath()} that performs auth then the WS upgrade. The app
     * owns the {@code HttpServer}; {@link #close()} does NOT close it.
     */
    void mount(Router router);

    /**
     * Mount onto an existing {@link HttpServer} (for apps without vertx-web).
     * Installs a handshake/auth filter + a webSocketHandler on {@code options.getPath()}.
     * The app owns the server; {@link #close()} does NOT close it.
     */
    void mount(HttpServer server);

    /** The resolved WebSocket path. */
    String path();

    /**
     * GO_AWAY + close all live sessions, and close the owned HttpServer &amp;
     * NetClient (never a mounted server).
     */
    Future<Void> close();

    // ---- lifecycle / metrics hooks (fluent, optional) ----

    TunnelServer onSession(Handler<SessionEvent> handler);

    TunnelServer onSessionClose(Handler<SessionEvent> handler);

    TunnelServer onStream(Handler<StreamEvent> handler);

    TunnelServer onStreamClose(Handler<StreamEvent> handler);

    TunnelServer onStreamError(Handler<StreamErrorEvent> handler);

    /** Live count of open tunnel sessions (WebSocket connections). */
    int activeSessions();

    /** Live count of open proxied streams across all sessions. */
    int activeStreams();
}
