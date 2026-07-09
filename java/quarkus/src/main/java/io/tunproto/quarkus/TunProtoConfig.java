package io.tunproto.quarkus;

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Configuration for the embedded tun-proto tunnel server, bound from
 * {@code tun-proto.*} properties. A developer only needs to know two things:
 * set an API key ({@code tun-proto.api-keys[0]=...}) and (optionally) lock down
 * egress with a {@link io.tunproto.vertx.TargetPolicy} bean. Nothing about
 * yamux, WebSocket upgrades, or the OpenRequest wire format is exposed.
 */
@ConfigMapping(prefix = "tun-proto")
public interface TunProtoConfig {

    /** Master switch. When {@code false}, the tunnel server is not started. */
    @WithDefault("true")
    boolean enabled();

    /** WebSocket upgrade path. Normal app routes are untouched. */
    @WithDefault("/tunnels")
    String path();

    /**
     * When set, the tunnel binds its OWN {@code HttpServer} on this port. When
     * unset (default), it MOUNTS on the Quarkus-managed HTTP server and shares
     * the app's port and lifecycle — the least-learning default.
     */
    OptionalInt port();

    /** Static Bearer allowlist. Any listed key authenticates an upgrade. */
    Optional<List<String>> apiKeys();

    /** Dev-only: accept every upgrade without checking the Bearer key. */
    @WithDefault("false")
    boolean authDisabled();

    /** Per-stream yamux window (bytes). Matches the Go client's 16 MiB default. */
    @WithDefault("16777216")
    int maxStreamWindow();

    /** Max concurrent tunnel sessions (WebSockets). 0 = unlimited. */
    @WithDefault("0")
    int maxSessions();

    /** Max concurrent proxied streams across all sessions. 0 = unlimited. */
    @WithDefault("0")
    int maxStreams();

    /** Timeout for dialing a target socket. */
    @WithDefault("10s")
    Duration dialTimeout();

    /** How often to send a keepalive PING on each session. */
    @WithDefault("30s")
    Duration keepAliveInterval();

    /** Session torn down if no PONG arrives within this timeout. */
    @WithDefault("40s")
    Duration keepAliveTimeout();
}
