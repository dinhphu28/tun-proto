package io.tunproto.vertx.events;

/**
 * Fired when a proxied stream (one multiplexed connection to a target) opens or
 * closes.
 */
public final class StreamEvent {

    private final String sessionId;
    private final long streamId;
    private final String targetHost;
    private final int targetPort;

    public StreamEvent(String sessionId, long streamId, String targetHost, int targetPort) {
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.targetHost = targetHost;
        this.targetPort = targetPort;
    }

    public String sessionId() {
        return sessionId;
    }

    public long streamId() {
        return streamId;
    }

    /** Resolved target host, or {@code null} if the OpenRequest was not yet parsed. */
    public String targetHost() {
        return targetHost;
    }

    /** Resolved target port, or {@code 0} if the OpenRequest was not yet parsed. */
    public int targetPort() {
        return targetPort;
    }

    @Override
    public String toString() {
        return "StreamEvent[sessionId=" + sessionId + ", streamId=" + streamId
                + ", target=" + targetHost + ":" + targetPort + "]";
    }
}
