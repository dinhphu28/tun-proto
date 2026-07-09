package io.tunproto.yamux;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;

/**
 * Incremental, buffered reader for the length-prefixed OpenRequest that begins
 * every accepted yamux stream (SPEC §5). Mirrors node/src/open-request.js:
 * 4-byte BE length N (1..4096), then N UTF-8 JSON bytes {network, address}.
 * Bytes after 4+N become {@code leftover} (the start of the tunneled payload).
 *
 * <p>Threading: single-threaded, driven from the stream's dataHandler.</p>
 */
public final class OpenRequestReader {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Completion result. */
    public static final class Result {
        public final OpenRequest request;
        /** Bytes after the JSON (may be empty); OWNED by the caller (must release). */
        public final ByteBuf leftover;
        /**
         * Total DATA payload bytes the reader consumed to reach completion:
         * 4 + jsonLen + leftover.readableBytes(). These bytes already decremented
         * the stream recvWindow; the splicer MUST replenish them via
         * stream.consumed(consumedBytes) as the target drains (SPEC Blocker 3).
         */
        public final int consumedBytes;

        Result(OpenRequest request, ByteBuf leftover, int consumedBytes) {
            this.request = request;
            this.leftover = leftover;
            this.consumedBytes = consumedBytes;
        }
    }

    private final int maxSize;
    private final ByteBuf accum;
    private int needLen = -1;
    private boolean released = false;
    private int fedBytes = 0;

    public OpenRequestReader() {
        this(YamuxConstants.MAX_OPEN_REQUEST_SIZE);
    }

    public OpenRequestReader(int maxSize) {
        this.maxSize = maxSize;
        this.accum = Unpooled.buffer(256);
    }

    /**
     * Offer the next chunk of stream DATA. Ownership of {@code chunk} transfers to
     * the reader (it is copied then released). Returns a {@link Result} once the
     * OpenRequest is complete, else null.
     *
     * @throws ProtocolException on any violation.
     */
    public Result offer(ByteBuf chunk) {
        if (released) {
            chunk.release();
            throw new ProtocolException("reader already released", YamuxConstants.GOAWAY_INTERNAL);
        }
        try {
            fedBytes += chunk.readableBytes();
            accum.writeBytes(chunk, chunk.readerIndex(), chunk.readableBytes());
        } finally {
            chunk.release();
        }

        if (needLen == -1) {
            if (accum.readableBytes() < 4) {
                return null;
            }
            long n = accum.getInt(accum.readerIndex()) & 0xFFFFFFFFL;
            if (n == 0 || n > maxSize) {
                throw new ProtocolException("invalid open request size: " + n, YamuxConstants.GOAWAY_PROTO);
            }
            needLen = (int) n;
        }

        if (accum.readableBytes() < 4 + needLen) {
            return null;
        }

        int ri = accum.readerIndex();
        byte[] json = new byte[needLen];
        accum.getBytes(ri + 4, json, 0, needLen);

        OpenRequest req;
        try {
            JsonNode node = MAPPER.readTree(new String(json, StandardCharsets.UTF_8));
            String network = node.hasNonNull("network") ? node.get("network").asText() : "";
            if (network.isEmpty()) {
                network = "tcp";
            }
            if (!"tcp".equals(network)) {
                throw new ProtocolException("unsupported network: " + network, YamuxConstants.GOAWAY_PROTO);
            }
            String address = node.hasNonNull("address") ? node.get("address").asText() : "";
            if (address.isEmpty()) {
                throw new ProtocolException("empty target address", YamuxConstants.GOAWAY_PROTO);
            }
            req = new OpenRequest(network, address);
        } catch (ProtocolException e) {
            throw e;
        } catch (Exception e) {
            throw new ProtocolException("invalid open request JSON", YamuxConstants.GOAWAY_PROTO);
        }

        int leftoverStart = ri + 4 + needLen;
        int leftoverLen = accum.writerIndex() - leftoverStart;
        ByteBuf leftover = Unpooled.buffer(Math.max(1, leftoverLen));
        if (leftoverLen > 0) {
            leftover.writeBytes(accum, leftoverStart, leftoverLen);
        }
        int consumedBytes = 4 + needLen + leftoverLen;

        released = true;
        accum.release();
        return new Result(req, leftover, consumedBytes);
    }

    /** Free internal accumulation on error/teardown. Idempotent. */
    public void release() {
        if (!released) {
            released = true;
            accum.release();
        }
    }
}
