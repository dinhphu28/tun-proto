package io.tunproto.vertx.events;

/**
 * Fired when a tunnel session (one WebSocket connection) opens or closes.
 */
public final class SessionEvent {

    private final String sessionId;
    private final String remoteAddress;

    public SessionEvent(String sessionId, String remoteAddress) {
        this.sessionId = sessionId;
        this.remoteAddress = remoteAddress;
    }

    /** Opaque, per-connection session id. */
    public String sessionId() {
        return sessionId;
    }

    /** Remote peer address ("host:port"), or {@code null}. */
    public String remoteAddress() {
        return remoteAddress;
    }

    @Override
    public String toString() {
        return "SessionEvent[sessionId=" + sessionId + ", remoteAddress=" + remoteAddress + "]";
    }
}
