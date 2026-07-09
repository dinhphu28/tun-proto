package io.tunproto.vertx.internal;

import io.tunproto.vertx.AuthContext;
import io.tunproto.vertx.Authenticator;
import io.tunproto.vertx.TunnelOptions;
import io.tunproto.vertx.TunnelServer;
import io.tunproto.vertx.events.SessionEvent;
import io.tunproto.vertx.events.StreamErrorEvent;
import io.tunproto.vertx.events.StreamEvent;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.ServerWebSocketHandshake;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implementation of {@link TunnelServer}: option validation, the shared
 * async {@code NetClient}, auth + WS upgrade, per-session engine wiring, session
 * gauge, and lifecycle.
 */
public final class TunnelServerImpl implements TunnelServer {

    private static final Logger LOG = Logger.getLogger(TunnelServerImpl.class.getName());
    private static final String BEARER_PREFIX = "bearer ";

    private final Vertx vertx;
    private final TunnelOptions options;
    private final NetClient netClient;

    private volatile HttpServer ownedServer; // set only by listen()

    // Live gauges.
    private final AtomicInteger sessionCount = new AtomicInteger();
    private final AtomicInteger globalStreamCount = new AtomicInteger();
    private final AtomicLong sessionSeq = new AtomicLong();
    private final Set<TunnelSession> liveSessions = Collections.newSetFromMap(new ConcurrentHashMap<>());

    // Hooks.
    private volatile Handler<SessionEvent> onSession;
    private volatile Handler<SessionEvent> onSessionClose;
    private volatile Handler<StreamEvent> onStream;
    private volatile Handler<StreamEvent> onStreamClose;
    private volatile Handler<StreamErrorEvent> onStreamError;

    private TunnelServerImpl(Vertx vertx, TunnelOptions options, NetClient netClient) {
        this.vertx = vertx;
        this.options = options;
        this.netClient = netClient;
    }

    public static TunnelServer create(Vertx vertx, TunnelOptions options) {
        validate(options);

        NetClientOptions ncOpts = new NetClientOptions()
                .setConnectTimeout((int) Math.max(1, options.getDialTimeout().toMillis()))
                .setReconnectAttempts(0);
        NetClient netClient = vertx.createNetClient(ncOpts);

        return new TunnelServerImpl(vertx, options, netClient);
    }

    private static void validate(TunnelOptions options) {
        int modes = 0;
        if (!options.getApiKeys().isEmpty()) {
            modes++;
        }
        if (options.getAuthenticator() != null) {
            modes++;
        }
        if (options.isAuthDisabled()) {
            modes++;
        }
        if (modes == 0) {
            throw new IllegalArgumentException(
                    "no auth configured; set apiKeys, an Authenticator, or authDisabled=true");
        }
        if (modes > 1) {
            throw new IllegalArgumentException(
                    "exactly one auth mode allowed; configure only one of {apiKeys, authenticator, authDisabled}");
        }
        if (options.getAllowTarget() == null) {
            LOG.warning("tun-proto: no TargetPolicy set - the tunnel will dial ANY host:port a client requests "
                    + "(SSRF risk). Set TunnelOptions.setAllowTarget(...) to restrict egress.");
        }
    }

    // ---- listen (owned server) ----

    @Override
    public Future<TunnelServer> listen(int port) {
        return listen(port, "0.0.0.0");
    }

    @Override
    public Future<TunnelServer> listen(int port, String host) {
        HttpServer server = vertx.createHttpServer();
        this.ownedServer = server;
        installOnServer(server);
        return server.listen(port, host).map(s -> this);
    }

    /**
     * Install the handshake auth filter + the WS accept handler on an HttpServer.
     * In Vert.x 4.5 the {@code webSocketHandshakeHandler} decides accept/reject;
     * once accepted, the {@code webSocketHandler} receives the upgraded
     * {@link ServerWebSocket}. Both must be present.
     */
    private void installOnServer(HttpServer server) {
        server.webSocketHandshakeHandler(this::handleHandshake);
        server.webSocketHandler(this::handleWebSocket);
    }

    private void handleWebSocket(ServerWebSocket ws) {
        String remote = ws.remoteAddress() == null ? null : ws.remoteAddress().toString();
        startSession(ws, remote);
    }

    // ---- mount (existing HttpServer, no vertx-web) ----

    @Override
    public void mount(HttpServer server) {
        installOnServer(server);
    }

    // ---- mount (vertx-web Router) ----

    @Override
    public void mount(Router router) {
        router.route(path()).handler(this::handleRouterUpgrade);
    }

    private void handleRouterUpgrade(RoutingContext ctx) {
        if (!"websocket".equalsIgnoreCase(ctx.request().getHeader("Upgrade"))) {
            ctx.next();
            return;
        }
        MultiMap headers = ctx.request().headers();
        String remote = ctx.request().remoteAddress() == null ? null : ctx.request().remoteAddress().toString();
        String bearer = extractBearer(headers);

        authorize(bearer, headers, remote).onComplete(ar -> {
            boolean ok = ar.succeeded() && Boolean.TRUE.equals(ar.result());
            if (!ok) {
                ctx.response().setStatusCode(401).end();
                return;
            }
            if (atSessionCap()) {
                ctx.response().setStatusCode(503).end();
                return;
            }
            ctx.request().toWebSocket().onComplete(wsAr -> {
                if (wsAr.succeeded()) {
                    startSession(wsAr.result(), remote);
                } else {
                    ctx.response().setStatusCode(400).end();
                }
            });
        });
    }

    // ---- raw HttpServer handshake + WS handlers ----

    private void handleHandshake(ServerWebSocketHandshake handshake) {
        if (!path().equals(handshake.path())) {
            handshake.reject(404);
            return;
        }
        MultiMap headers = handshake.headers();
        String remote = handshake.remoteAddress() == null ? null : handshake.remoteAddress().toString();
        String bearer = extractBearer(headers);

        authorize(bearer, headers, remote).onComplete(ar -> {
            boolean ok = ar.succeeded() && Boolean.TRUE.equals(ar.result());
            if (!ok) {
                handshake.reject(401);
                return;
            }
            if (atSessionCap()) {
                handshake.reject(503);
                return;
            }
            // Accept: the webSocketHandler will receive the upgraded ServerWebSocket.
            handshake.accept();
        });
    }

    // ---- auth core ----

    private Future<Boolean> authorize(String bearer, MultiMap headers, String remote) {
        if (options.isAuthDisabled()) {
            return Future.succeededFuture(Boolean.TRUE);
        }
        Authenticator authenticator = options.getAuthenticator();
        if (authenticator != null) {
            AuthContext authCtx = new AuthContext() {
                @Override public MultiMap headers() { return headers; }
                @Override public String remoteAddress() { return remote; }
                @Override public String path() { return path(); }
            };
            Future<Boolean> f = authenticator.authenticate(bearer, authCtx);
            return f == null ? Future.succeededFuture(Boolean.FALSE) : f;
        }
        // Static allowlist (constant-time compare).
        if (bearer == null) {
            return Future.succeededFuture(Boolean.FALSE);
        }
        boolean match = false;
        byte[] provided = bearer.getBytes(StandardCharsets.UTF_8);
        for (String key : options.getApiKeys()) {
            if (constantTimeEquals(provided, key.getBytes(StandardCharsets.UTF_8))) {
                match = true;
            }
        }
        return Future.succeededFuture(match);
    }

    private static String extractBearer(MultiMap headers) {
        String auth = headers.get("Authorization");
        if (auth == null) {
            return null;
        }
        String trimmed = auth.trim();
        if (trimmed.length() < BEARER_PREFIX.length()) {
            return null;
        }
        String scheme = trimmed.substring(0, BEARER_PREFIX.length());
        if (!scheme.equalsIgnoreCase(BEARER_PREFIX)) {
            return null;
        }
        String token = trimmed.substring(BEARER_PREFIX.length()).trim();
        return token.isEmpty() ? null : token;
    }

    private static boolean constantTimeEquals(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return false;
        }
        int r = 0;
        for (int i = 0; i < a.length; i++) {
            r |= a[i] ^ b[i];
        }
        return r == 0;
    }

    private boolean atSessionCap() {
        int max = options.getMaxSessions();
        return max > 0 && sessionCount.get() >= max;
    }

    // ---- session lifecycle ----

    private void startSession(ServerWebSocket ws, String remote) {
        // Double-check cap after upgrade (race window).
        if (atSessionCap()) {
            ws.close((short) 1013, "server busy");
            return;
        }
        sessionCount.incrementAndGet();
        String sessionId = "s-" + sessionSeq.incrementAndGet();

        if (options.getIdleTimeout() != null && !options.getIdleTimeout().isZero()) {
            // Vert.x WS idle handling is via server options; nothing per-ws here.
        }

        TunnelSession[] holder = new TunnelSession[1];
        TunnelSession ts = new TunnelSession(
                vertx, ws, options, netClient, sessionId,
                options.getMaxStreams(), globalStreamCount,
                onStream, onStreamClose, onStreamError);
        holder[0] = ts;
        liveSessions.add(ts);

        // Decrement gauge + fire close hook exactly once when the WS closes.
        ws.closeHandler(v -> {
            if (liveSessions.remove(ts)) {
                sessionCount.decrementAndGet();
                Handler<SessionEvent> h = onSessionClose;
                if (h != null) {
                    h.handle(new SessionEvent(sessionId, remote));
                }
            }
            ts.teardown();
        });

        ts.start();

        Handler<SessionEvent> h = onSession;
        if (h != null) {
            h.handle(new SessionEvent(sessionId, remote));
        }
    }

    // ---- hooks ----

    @Override
    public TunnelServer onSession(Handler<SessionEvent> handler) {
        this.onSession = handler;
        return this;
    }

    @Override
    public TunnelServer onSessionClose(Handler<SessionEvent> handler) {
        this.onSessionClose = handler;
        return this;
    }

    @Override
    public TunnelServer onStream(Handler<StreamEvent> handler) {
        this.onStream = handler;
        return this;
    }

    @Override
    public TunnelServer onStreamClose(Handler<StreamEvent> handler) {
        this.onStreamClose = handler;
        return this;
    }

    @Override
    public TunnelServer onStreamError(Handler<StreamErrorEvent> handler) {
        this.onStreamError = handler;
        return this;
    }

    @Override
    public int activeSessions() {
        return sessionCount.get();
    }

    @Override
    public int activeStreams() {
        int total = globalStreamCount.get();
        if (total > 0) {
            return total;
        }
        // maxStreams unset => globalStreamCount is not incremented; sum per-session.
        int sum = 0;
        for (TunnelSession ts : liveSessions) {
            sum += ts.activeStreams();
        }
        return sum;
    }

    @Override
    public String path() {
        return options.getPath();
    }

    @Override
    public Future<Void> close() {
        // GO_AWAY + close all live sessions.
        for (TunnelSession ts : liveSessions.toArray(new TunnelSession[0])) {
            try {
                ts.shutdown();
            } catch (Exception e) {
                LOG.log(Level.FINE, "error shutting down session", e);
            }
        }
        liveSessions.clear();

        Future<Void> serverClose = Future.succeededFuture();
        HttpServer srv = this.ownedServer;
        if (srv != null) {
            serverClose = srv.close();
        }
        Future<Void> ncClose = netClient.close();
        return Future.join(serverClose, ncClose).mapEmpty();
    }
}
