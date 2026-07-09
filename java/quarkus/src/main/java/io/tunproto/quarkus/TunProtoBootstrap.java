package io.tunproto.quarkus;

import io.quarkus.runtime.ShutdownEvent;
import io.quarkus.runtime.StartupEvent;
import io.tunproto.vertx.Authenticator;
import io.tunproto.vertx.TargetPolicy;
import io.tunproto.vertx.TunnelOptions;
import io.tunproto.vertx.TunnelServer;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Auto-starts the embedded tun-proto tunnel server on application startup and
 * shuts it down cleanly on application stop.
 *
 * <p><b>Integration is one line of config.</b> Add this library as a dependency
 * and set {@code tun-proto.api-keys[0]=my-secret-key}. On {@link StartupEvent}
 * the tunnel mounts on the Quarkus HTTP server (sharing the app's port) and only
 * claims WebSocket upgrades on {@code tun-proto.path} ({@code /tunnels} by
 * default). All normal app routes are untouched. Developers need to know
 * nothing about yamux, WebSocket framing, or the OpenRequest wire format — the
 * engine from {@code tun-proto-vertx} handles all of it.</p>
 *
 * <p>Optional CDI hooks: define an {@code @ApplicationScoped}
 * {@link Authenticator} or {@link TargetPolicy} bean and it is auto-wired.</p>
 */
@ApplicationScoped
public class TunProtoBootstrap {

    private static final Logger LOG = Logger.getLogger(TunProtoBootstrap.class);

    @Inject
    Vertx vertx; // Quarkus-managed Vert.x

    @Inject
    TunProtoConfig config;

    @Inject
    Instance<Router> router; // from quarkus-vertx-http; used when mounting on the app port

    @Inject
    Instance<Authenticator> authenticator; // optional custom auth

    @Inject
    Instance<TargetPolicy> targetPolicy; // optional egress guard

    private volatile TunnelServer server;

    void onStart(@Observes StartupEvent ev) {
        if (!config.enabled()) {
            LOG.info("tun-proto: disabled (tun-proto.enabled=false)");
            return;
        }

        TunnelOptions opts = new TunnelOptions()
                .setPath(config.path())
                .setAuthDisabled(config.authDisabled())
                .setMaxStreamWindow(config.maxStreamWindow())
                .setMaxSessions(config.maxSessions())
                .setMaxStreams(config.maxStreams())
                .setDialTimeout(config.dialTimeout())
                .setKeepAliveInterval(config.keepAliveInterval())
                .setKeepAliveTimeout(config.keepAliveTimeout());

        config.apiKeys().ifPresent(opts::setApiKeys);

        // Optional CDI hooks take precedence over static config. Only one auth
        // mode may be active; a custom Authenticator overrides the key allowlist.
        if (authenticator.isResolvable()) {
            opts.setAuthenticator(authenticator.get());
            opts.setApiKeys(java.util.List.of()); // let the authenticator be the single auth mode
        }
        if (targetPolicy.isResolvable()) {
            opts.setAllowTarget(targetPolicy.get());
        }

        TunnelServer created;
        try {
            created = TunnelServer.create(vertx, opts);
        } catch (IllegalArgumentException e) {
            // Most commonly: no auth configured. Fail fast with a clear message.
            LOG.errorf("tun-proto: not started — %s. Set tun-proto.api-keys[0]=..., "
                    + "provide an Authenticator bean, or set tun-proto.auth-disabled=true (dev only).",
                    e.getMessage());
            throw e;
        }
        this.server = created;

        if (config.port().isPresent()) {
            int port = config.port().getAsInt();
            created.listen(port).onComplete(ar -> {
                if (ar.succeeded()) {
                    LOG.infof("tun-proto: tunnel listening on its own port ws://:%d%s", port, created.path());
                } else {
                    LOG.error("tun-proto: failed to bind own port", ar.cause());
                }
            });
        } else {
            created.mount(router.get());
            LOG.infof("tun-proto: tunnel mounted on the app HTTP server at path %s "
                    + "(shares the app port; WebSocket upgrades only)", created.path());
        }
    }

    void onStop(@Observes ShutdownEvent ev) {
        TunnelServer s = this.server;
        if (s != null) {
            s.close();
            this.server = null;
        }
    }

    /**
     * The running tunnel server, injectable so the app can read
     * {@code activeSessions()}/{@code activeStreams()} for metrics or attach
     * lifecycle handlers. May be {@code null} if the tunnel is disabled.
     */
    @Produces
    @ApplicationScoped
    TunnelServer tunnelServer() {
        return server;
    }
}
