package io.tunproto.yamux;

/**
 * yamux frame types. See SPEC.md §4.
 */
public enum FrameType {
    DATA(0),
    WINDOW_UPDATE(1),
    PING(2),
    GO_AWAY(3);

    public final int value;

    FrameType(int v) {
        this.value = v;
    }

    public static FrameType from(int v) {
        switch (v) {
            case 0: return DATA;
            case 1: return WINDOW_UPDATE;
            case 2: return PING;
            case 3: return GO_AWAY;
            default:
                throw new ProtocolException("unknown frame type: " + v, YamuxConstants.GOAWAY_PROTO);
        }
    }
}
