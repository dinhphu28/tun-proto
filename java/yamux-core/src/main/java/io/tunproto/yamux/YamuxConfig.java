package io.tunproto.yamux;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;

import java.time.Duration;

/**
 * Immutable yamux session configuration. Build via {@link #builder()}.
 */
public final class YamuxConfig {
    /** Max per-stream window (default 16 MiB, clamped min 256 KiB). */
    public final int maxStreamWindow;
    /** Initial per-stream window (fixed 256 KiB). */
    public final int initialWindow;
    /** Interval between our own keepalive pings. */
    public final Duration keepAliveInterval;
    /** Teardown if no pong within this timeout after a ping. */
    public final Duration keepAliveTimeout;
    /** Per-session concurrent stream cap; 0 = unlimited. */
    public final int maxConcurrentStreams;
    /** Allocator for outbound frames and the receive accumulator. */
    public final ByteBufAllocator allocator;

    private YamuxConfig(Builder b) {
        int max = Math.max(YamuxConstants.INITIAL_WINDOW, b.maxStreamWindow);
        this.maxStreamWindow = max;
        this.initialWindow = YamuxConstants.INITIAL_WINDOW;
        this.keepAliveInterval = b.keepAliveInterval;
        this.keepAliveTimeout = b.keepAliveTimeout;
        this.maxConcurrentStreams = b.maxConcurrentStreams;
        this.allocator = b.allocator;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private int maxStreamWindow = YamuxConstants.DEFAULT_MAX_WINDOW;
        private Duration keepAliveInterval = Duration.ofSeconds(30);
        private Duration keepAliveTimeout = Duration.ofSeconds(40);
        private int maxConcurrentStreams = 0;
        private ByteBufAllocator allocator = UnpooledByteBufAllocator.DEFAULT;

        public Builder maxStreamWindow(int v) { this.maxStreamWindow = v; return this; }
        public Builder keepAliveInterval(Duration v) { this.keepAliveInterval = v; return this; }
        public Builder keepAliveTimeout(Duration v) { this.keepAliveTimeout = v; return this; }
        public Builder maxConcurrentStreams(int v) { this.maxConcurrentStreams = v; return this; }
        public Builder allocator(ByteBufAllocator v) { this.allocator = v; return this; }

        public YamuxConfig build() {
            return new YamuxConfig(this);
        }
    }
}
