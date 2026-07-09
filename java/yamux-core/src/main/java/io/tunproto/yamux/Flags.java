package io.tunproto.yamux;

/**
 * yamux frame flags bitmask. See SPEC.md §4.
 */
public final class Flags {
    public static final int SYN = 1;
    public static final int ACK = 2;
    public static final int FIN = 4;
    public static final int RST = 8;

    public static boolean has(int flags, int flag) {
        return (flags & flag) != 0;
    }

    private Flags() {}
}
