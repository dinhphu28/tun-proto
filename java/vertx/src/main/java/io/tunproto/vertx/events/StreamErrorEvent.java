package io.tunproto.vertx.events;

/**
 * Fired when a proxied stream is rejected or fails: egress-policy denial, dial
 * failure, over-limit, or a protocol error while reading the OpenRequest.
 */
public final class StreamErrorEvent {

    private final String sessionId;
    private final long streamId;
    private final String targetAddress;
    private final Throwable cause;

    public StreamErrorEvent(String sessionId, long streamId, String targetAddress, Throwable cause) {
        this.sessionId = sessionId;
        this.streamId = streamId;
        this.targetAddress = targetAddress;
        this.cause = cause;
    }

    public String sessionId() {
        return sessionId;
    }

    public long streamId() {
        return streamId;
    }

    /** Target "host:port" if known, else {@code null}. */
    public String targetAddress() {
        return targetAddress;
    }

    /** Failure cause. */
    public Throwable cause() {
        return cause;
    }

    @Override
    public String toString() {
        return "StreamErrorEvent[sessionId=" + sessionId + ", streamId=" + streamId
                + ", target=" + targetAddress + ", cause=" + (cause == null ? null : cause.getMessage()) + "]";
    }
}
