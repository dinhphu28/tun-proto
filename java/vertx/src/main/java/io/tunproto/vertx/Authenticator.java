package io.tunproto.vertx;

import io.vertx.core.Future;

/**
 * Pluggable, non-blocking authenticator for WebSocket upgrades. Given the
 * extracted Bearer key (may be {@code null} if the header was absent) and the
 * {@link AuthContext}, complete the returned Future with {@code true} to accept
 * the upgrade or {@code false} to reject it (HTTP 401).
 *
 * <p>MUST be non-blocking: it runs on the connection's event loop. Never block
 * (no {@code .result()}, {@code .get()}, {@code Thread.sleep}); return a Future
 * that completes asynchronously.</p>
 */
@FunctionalInterface
public interface Authenticator {
    Future<Boolean> authenticate(String bearerKey, AuthContext ctx);
}
