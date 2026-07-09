package io.tunproto.vertx;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Data holder for {@link TunnelServer} configuration. Fluent setters; all fields
 * have sensible defaults. Validation happens in {@link TunnelServer#create}.
 */
public final class TunnelOptions {

    private String path = "/tunnels";
    private final List<String> apiKeys = new ArrayList<>();
    private Authenticator authenticator;
    private boolean authDisabled = false;
    private TargetPolicy allowTarget; // null => allow-all + one-time SSRF warn

    private int maxStreamWindow = 16 * 1024 * 1024;      // 16 MiB, matches Go client
    private int maxSessions = 0;                           // 0 = unlimited
    private int maxStreams = 0;                            // 0 = unlimited (global)
    private int maxConcurrentStreamsPerSession = 0;        // 0 = unlimited

    private Duration dialTimeout = Duration.ofSeconds(10);
    private Duration keepAliveInterval = Duration.ofSeconds(30);
    private Duration keepAliveTimeout = Duration.ofSeconds(40);
    private Duration idleTimeout = Duration.ZERO;          // 0 = off

    public TunnelOptions() {
    }

    // ---- path ----

    public String getPath() {
        return path;
    }

    public TunnelOptions setPath(String path) {
        this.path = Objects.requireNonNull(path, "path");
        return this;
    }

    // ---- auth: static keys ----

    public List<String> getApiKeys() {
        return apiKeys;
    }

    public TunnelOptions addApiKey(String key) {
        this.apiKeys.add(Objects.requireNonNull(key, "key"));
        return this;
    }

    public TunnelOptions setApiKeys(List<String> keys) {
        this.apiKeys.clear();
        if (keys != null) {
            this.apiKeys.addAll(keys);
        }
        return this;
    }

    // ---- auth: custom authenticator ----

    public Authenticator getAuthenticator() {
        return authenticator;
    }

    public TunnelOptions setAuthenticator(Authenticator authenticator) {
        this.authenticator = authenticator;
        return this;
    }

    // ---- auth: disabled (dev only) ----

    public boolean isAuthDisabled() {
        return authDisabled;
    }

    public TunnelOptions setAuthDisabled(boolean authDisabled) {
        this.authDisabled = authDisabled;
        return this;
    }

    // ---- egress policy ----

    public TargetPolicy getAllowTarget() {
        return allowTarget;
    }

    public TunnelOptions setAllowTarget(TargetPolicy allowTarget) {
        this.allowTarget = allowTarget;
        return this;
    }

    // ---- yamux / limits ----

    public int getMaxStreamWindow() {
        return maxStreamWindow;
    }

    public TunnelOptions setMaxStreamWindow(int maxStreamWindow) {
        this.maxStreamWindow = maxStreamWindow;
        return this;
    }

    public int getMaxSessions() {
        return maxSessions;
    }

    public TunnelOptions setMaxSessions(int maxSessions) {
        this.maxSessions = maxSessions;
        return this;
    }

    public int getMaxStreams() {
        return maxStreams;
    }

    public TunnelOptions setMaxStreams(int maxStreams) {
        this.maxStreams = maxStreams;
        return this;
    }

    public int getMaxConcurrentStreamsPerSession() {
        return maxConcurrentStreamsPerSession;
    }

    public TunnelOptions setMaxConcurrentStreamsPerSession(int maxConcurrentStreamsPerSession) {
        this.maxConcurrentStreamsPerSession = maxConcurrentStreamsPerSession;
        return this;
    }

    // ---- timeouts ----

    public Duration getDialTimeout() {
        return dialTimeout;
    }

    public TunnelOptions setDialTimeout(Duration dialTimeout) {
        this.dialTimeout = Objects.requireNonNull(dialTimeout, "dialTimeout");
        return this;
    }

    public Duration getKeepAliveInterval() {
        return keepAliveInterval;
    }

    public TunnelOptions setKeepAliveInterval(Duration keepAliveInterval) {
        this.keepAliveInterval = Objects.requireNonNull(keepAliveInterval, "keepAliveInterval");
        return this;
    }

    public Duration getKeepAliveTimeout() {
        return keepAliveTimeout;
    }

    public TunnelOptions setKeepAliveTimeout(Duration keepAliveTimeout) {
        this.keepAliveTimeout = Objects.requireNonNull(keepAliveTimeout, "keepAliveTimeout");
        return this;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public TunnelOptions setIdleTimeout(Duration idleTimeout) {
        this.idleTimeout = Objects.requireNonNull(idleTimeout, "idleTimeout");
        return this;
    }
}
