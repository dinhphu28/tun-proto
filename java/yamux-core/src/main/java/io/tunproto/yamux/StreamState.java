package io.tunproto.yamux;

/**
 * Half-close state tracking for a {@link YamuxStream}.
 */
public enum StreamState {
    OPEN,
    LOCAL_CLOSED,
    REMOTE_CLOSED,
    CLOSED
}
