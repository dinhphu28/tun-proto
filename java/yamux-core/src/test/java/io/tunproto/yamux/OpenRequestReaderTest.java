package io.tunproto.yamux;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class OpenRequestReaderTest {

    private static ByteBuf buf(byte[] b) {
        ByteBuf x = Unpooled.buffer(b.length);
        x.writeBytes(b);
        return x;
    }

    private static byte[] withLen(String json) {
        byte[] jb = json.getBytes(StandardCharsets.UTF_8);
        ByteBuf b = Unpooled.buffer(4 + jb.length);
        b.writeInt(jb.length);
        b.writeBytes(jb);
        byte[] out = new byte[b.readableBytes()];
        b.readBytes(out);
        b.release();
        return out;
    }

    @Test
    void parsesSingleChunk() {
        OpenRequestReader r = new OpenRequestReader();
        OpenRequestReader.Result res = r.offer(buf(withLen("{\"network\":\"tcp\",\"address\":\"127.0.0.1:5432\"}")));
        assertNotNull(res);
        assertEquals("tcp", res.request.network());
        assertEquals("127.0.0.1", res.request.host());
        assertEquals(5432, res.request.port());
        assertEquals(0, res.leftover.readableBytes());
        res.leftover.release();
    }

    @Test
    void defaultsNetworkTcp() {
        OpenRequestReader r = new OpenRequestReader();
        OpenRequestReader.Result res = r.offer(buf(withLen("{\"address\":\"example.com:80\"}")));
        assertNotNull(res);
        assertEquals("tcp", res.request.network());
        assertEquals("example.com", res.request.host());
        assertEquals(80, res.request.port());
        res.leftover.release();
    }

    @Test
    void splitAcrossChunks() {
        byte[] full = withLen("{\"network\":\"tcp\",\"address\":\"h:1234\"}");
        byte[] trailing = "PAYLOAD".getBytes(StandardCharsets.UTF_8);
        byte[] all = new byte[full.length + trailing.length];
        System.arraycopy(full, 0, all, 0, full.length);
        System.arraycopy(trailing, 0, all, full.length, trailing.length);

        OpenRequestReader r = new OpenRequestReader();
        // feed length prefix minus a byte
        assertNull(r.offer(buf(new byte[] {all[0], all[1], all[2]})));
        // feed rest of prefix + half the JSON
        int mid = full.length / 2;
        byte[] part2 = new byte[mid - 3];
        System.arraycopy(all, 3, part2, 0, part2.length);
        assertNull(r.offer(buf(part2)));
        // feed the remainder including trailing payload
        byte[] part3 = new byte[all.length - mid];
        System.arraycopy(all, mid, part3, 0, part3.length);
        OpenRequestReader.Result res = r.offer(buf(part3));
        assertNotNull(res);
        assertEquals("h", res.request.host());
        assertEquals(1234, res.request.port());
        byte[] lo = new byte[res.leftover.readableBytes()];
        res.leftover.readBytes(lo);
        assertArrayEquals(trailing, lo);
        res.leftover.release();
    }

    @Test
    void leftoverPreserved() {
        byte[] full = withLen("{\"address\":\"h:9\"}");
        byte[] trailing = {1, 2, 3, 4, 5};
        byte[] all = new byte[full.length + trailing.length];
        System.arraycopy(full, 0, all, 0, full.length);
        System.arraycopy(trailing, 0, all, full.length, trailing.length);
        OpenRequestReader r = new OpenRequestReader();
        OpenRequestReader.Result res = r.offer(buf(all));
        assertNotNull(res);
        byte[] lo = new byte[res.leftover.readableBytes()];
        res.leftover.readBytes(lo);
        assertArrayEquals(trailing, lo);
        res.leftover.release();
    }

    @Test
    void rejectsZeroLength() {
        OpenRequestReader r = new OpenRequestReader();
        ByteBuf b = Unpooled.buffer(4);
        b.writeInt(0);
        assertThrows(ProtocolException.class, () -> r.offer(b));
    }

    @Test
    void rejectsTooLarge() {
        OpenRequestReader r = new OpenRequestReader();
        ByteBuf b = Unpooled.buffer(4);
        b.writeInt(4097);
        assertThrows(ProtocolException.class, () -> r.offer(b));
    }

    @Test
    void rejectsBadJson() {
        OpenRequestReader r = new OpenRequestReader();
        assertThrows(ProtocolException.class, () -> r.offer(buf(withLen("{not json"))));
    }

    @Test
    void rejectsNonTcp() {
        OpenRequestReader r = new OpenRequestReader();
        assertThrows(ProtocolException.class,
                () -> r.offer(buf(withLen("{\"network\":\"udp\",\"address\":\"h:1\"}"))));
    }

    @Test
    void rejectsEmptyAddress() {
        OpenRequestReader r = new OpenRequestReader();
        assertThrows(ProtocolException.class,
                () -> r.offer(buf(withLen("{\"network\":\"tcp\",\"address\":\"\"}"))));
    }

    @Test
    void consumedBytesAccounting() {
        String json = "{\"network\":\"tcp\",\"address\":\"h:1234\"}";
        byte[] full = withLen(json);
        byte[] trailing = "abcdef".getBytes(StandardCharsets.UTF_8);
        byte[] all = new byte[full.length + trailing.length];
        System.arraycopy(full, 0, all, 0, full.length);
        System.arraycopy(trailing, 0, all, full.length, trailing.length);

        OpenRequestReader r = new OpenRequestReader();
        OpenRequestReader.Result res = r.offer(buf(all));
        assertNotNull(res);
        int jsonLen = json.getBytes(StandardCharsets.UTF_8).length;
        assertEquals(4 + jsonLen + trailing.length, res.consumedBytes);
        assertEquals(all.length, res.consumedBytes);
        res.leftover.release();
    }

    @Test
    void ipv6BracketedAddress() {
        OpenRequestReader r = new OpenRequestReader();
        OpenRequestReader.Result res = r.offer(buf(withLen("{\"address\":\"[::1]:5432\"}")));
        assertNotNull(res);
        assertEquals("::1", res.request.host());
        assertEquals(5432, res.request.port());
        res.leftover.release();
    }
}
