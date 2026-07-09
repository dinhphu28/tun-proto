package io.tunproto.yamux;

/**
 * Thrown on any yamux protocol violation. Carries the GO_AWAY error code the
 * session should emit before tearing down.
 */
public final class ProtocolException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    /** GOAWAY_PROTO or GOAWAY_INTERNAL. */
    public final long goAwayCode;

    public ProtocolException(String msg, long code) {
        super(msg);
        this.goAwayCode = code;
    }
}
