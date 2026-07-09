package io.tunproto.vertx;

import io.vertx.core.Vertx;
import io.vertx.core.net.NetServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static io.tunproto.vertx.TestSupport.await;
import static io.tunproto.vertx.TestSupport.listenEphemeral;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EndToEndSpliceTest {

    private Vertx vertx;

    @BeforeEach
    void setup() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void teardown() throws Exception {
        await(vertx.close(), 5);
    }

    /** Start an echo TCP server; return its bound port. */
    private int startEchoServer() throws Exception {
        NetServer echo = vertx.createNetServer();
        echo.connectHandler(sock -> sock.handler(sock::write));
        return await(echo.listen(0, "127.0.0.1"), 5).actualPort();
    }

    private YamuxTestClient connectClient(int tunnelPort) throws Exception {
        YamuxTestClient client = new YamuxTestClient(vertx);
        await(client.connect(tunnelPort, "/tunnels", null), 5);
        return client;
    }

    @Test
    void echoRoundTrip() throws Exception {
        int echoPort = startEchoServer();
        TunnelServer server = TunnelServer.create(vertx, new TunnelOptions().setAuthDisabled(true));
        int tunnelPort = listenEphemeral(server, 5);

        YamuxTestClient client = connectClient(tunnelPort);
        long sid = client.openStream("127.0.0.1", echoPort);
        YamuxTestClient.StreamState st = client.stream(sid);
        var done = st.awaitBytes("hello tunnel".length());
        client.sendString(sid, "hello tunnel");

        done.get(10, TimeUnit.SECONDS);
        assertFalse(st.rst, "stream must not be RST");
        assertEquals("hello tunnel", st.data.toString());

        await(client.close(), 5);
        await(server.close(), 5);
    }

    /**
     * Target that, on connect, streams {@code size} bytes back to the peer (DOWN
     * direction). This exercises the exact scenario the SYN window delta fixes: a
     * &gt;256 KiB download that must NOT stall at 262144 bytes.
     */
    private int startGeneratorServer(int size, char base) throws Exception {
        String payload = buildPayload(size, base);
        NetServer gen = vertx.createNetServer();
        gen.connectHandler(sock -> {
            // Ignore inbound; on first byte (the trigger), stream the payload down.
            sock.handler(b -> sock.write(io.vertx.core.buffer.Buffer.buffer(payload)));
        });
        return await(gen.listen(0, "127.0.0.1"), 5).actualPort();
    }

    @Test
    void largeDownloadAbove256KiB() throws Exception {
        // 1 MiB > 256 KiB initial window: proves the SYN delta window is honored
        // end-to-end (no stall at 262144 bytes) for the DOWN direction.
        int size = 1024 * 1024;
        String payload = buildPayload(size, 'a');
        int genPort = startGeneratorServer(size, 'a');
        TunnelServer server = TunnelServer.create(vertx, new TunnelOptions().setAuthDisabled(true));
        int tunnelPort = listenEphemeral(server, 5);

        YamuxTestClient client = connectClient(tunnelPort);
        long sid = client.openStream("127.0.0.1", genPort);
        YamuxTestClient.StreamState st = client.stream(sid);
        var done = st.awaitBytes(size);
        client.sendString(sid, "go"); // trigger the download

        done.get(30, TimeUnit.SECONDS);
        assertFalse(st.rst, "large download must not be RST");
        assertEquals(size, st.data.length(), "all bytes must arrive (no 256 KiB stall)");
        assertEquals(payload, st.data.toString());

        await(client.close(), 5);
        await(server.close(), 5);
    }

    @Test
    void egressPolicyDenyResetsStream() throws Exception {
        int echoPort = startEchoServer();
        TunnelServer server = TunnelServer.create(vertx, new TunnelOptions()
                .setAuthDisabled(true)
                .setAllowTarget((host, port) -> false));
        int tunnelPort = listenEphemeral(server, 5);

        YamuxTestClient client = connectClient(tunnelPort);
        long sid = client.openStream("127.0.0.1", echoPort);
        YamuxTestClient.StreamState st = client.stream(sid);
        st.finished.get(10, TimeUnit.SECONDS);
        assertTrue(st.rst, "egress-denied stream must be RST");

        await(client.close(), 5);
        await(server.close(), 5);
    }

    @Test
    void concurrentStreams() throws Exception {
        int echoPort = startEchoServer();
        TunnelServer server = TunnelServer.create(vertx, new TunnelOptions().setAuthDisabled(true));
        int tunnelPort = listenEphemeral(server, 5);

        YamuxTestClient client = connectClient(tunnelPort);
        int n = 5;
        long[] ids = new long[n];
        var dones = new java.util.concurrent.CompletableFuture[n];
        for (int i = 0; i < n; i++) {
            ids[i] = client.openStream("127.0.0.1", echoPort);
            String msg = "stream-" + i;
            dones[i] = client.stream(ids[i]).awaitBytes(msg.length());
            client.sendString(ids[i], msg);
        }
        for (int i = 0; i < n; i++) {
            dones[i].get(10, TimeUnit.SECONDS);
            YamuxTestClient.StreamState st = client.stream(ids[i]);
            assertFalse(st.rst);
            assertEquals("stream-" + i, st.data.toString());
        }

        await(client.close(), 5);
        await(server.close(), 5);
    }

    static String buildPayload(int size, char base) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append((char) (base + (i % 26)));
        }
        return sb.toString();
    }
}
