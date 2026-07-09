package io.tunproto.vertx;

import io.vertx.core.MultiMap;

/**
 * Context passed to a custom {@link Authenticator} for a pending WebSocket
 * upgrade. Exposes just enough of the HTTP request to make an auth decision
 * without coupling to Vert.x request internals.
 */
public interface AuthContext {

    /** All request headers of the upgrade request. */
    MultiMap headers();

    /** Remote peer address ("host:port"), or {@code null} if unavailable. */
    String remoteAddress();

    /** The WebSocket upgrade path (e.g. {@code /tunnels}). */
    String path();
}
