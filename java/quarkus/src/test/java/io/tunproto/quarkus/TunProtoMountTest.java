package io.tunproto.quarkus;

import io.quarkus.test.junit.QuarkusTest;
import io.tunproto.vertx.TunnelServer;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetServer;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Proves the tunnel auto-mounts on the Quarkus HTTP server (shared app port) and
 * splices bytes end-to-end: an echo round-trip and a &gt;256 KiB transfer that
 * must not stall at the 262144-byte initial-window boundary.
 */
@QuarkusTest
class TunProtoMountTest {

    @Inject
    Vertx vertx;

    @Inject
    TunnelServer tunnelServer; // produced by TunProtoBootstrap

    @ConfigProperty(name = "quarkus.http.test-port", defaultValue = "8081")
    int appPort;

    private int startEchoServer() throws Exception {
        NetServer echo = vertx.createNetServer();
        echo.connectHandler(sock -> sock.handler(sock::write));
        return await(echo.listen(0, "127.0.0.1")).actualPort();
    }

    @Test
    void tunnelServerIsProduced() {
        assertNotNull(tunnelServer, "TunnelServer must be produced and injectable");
        assertEquals("/tunnels", tunnelServer.path());
    }

    @Test
    void echoRoundTripThroughMountedTunnel() throws Exception {
        int echoPort = startEchoServer();

        YamuxWsClient client = new YamuxWsClient(vertx);
        await(client.connect(appPort, "/tunnels", null)); // auth-disabled in test config

        long sid = client.openStream("127.0.0.1", echoPort);
        YamuxWsClient.StreamState st = client.stream(sid);
        CompletableFuture<Void> done = st.awaitBytes("hello quarkus".length());
        client.sendString(sid, "hello quarkus");

        done.get(15, TimeUnit.SECONDS);
        assertFalse(st.rst, "stream must not be RST");
        assertEquals("hello quarkus", st.data.toString());

        await(client.close());
    }

    @Test
    void largeTransferAbove256KiB() throws Exception {
        // 512 KiB > 256 KiB initial window: proves the SYN-delta window is honored
        // end-to-end through the mounted tunnel (no stall at 262144 bytes).
        int size = 512 * 1024;
        String payload = buildPayload(size);

        NetServer gen = vertx.createNetServer();
        gen.connectHandler(sock ->
                sock.handler(b -> sock.write(io.vertx.core.buffer.Buffer.buffer(payload))));
        int genPort = await(gen.listen(0, "127.0.0.1")).actualPort();

        YamuxWsClient client = new YamuxWsClient(vertx);
        await(client.connect(appPort, "/tunnels", null));

        long sid = client.openStream("127.0.0.1", genPort);
        YamuxWsClient.StreamState st = client.stream(sid);
        CompletableFuture<Void> done = st.awaitBytes(size);
        client.sendString(sid, "go"); // trigger the download

        done.get(30, TimeUnit.SECONDS);
        assertFalse(st.rst, "large transfer must not be RST");
        assertEquals(size, st.data.length(), "all bytes must arrive (no 256 KiB stall)");
        assertEquals(payload, st.data.toString());

        await(client.close());
    }

    // ---- helpers ----

    private static <T> T await(io.vertx.core.Future<T> f) throws Exception {
        CompletableFuture<T> cf = new CompletableFuture<>();
        f.onComplete(ar -> {
            if (ar.succeeded()) {
                cf.complete(ar.result());
            } else {
                cf.completeExceptionally(ar.cause());
            }
        });
        return cf.get(15, TimeUnit.SECONDS);
    }

    static String buildPayload(int size) {
        StringBuilder sb = new StringBuilder(size);
        for (int i = 0; i < size; i++) {
            sb.append((char) ('a' + (i % 26)));
        }
        return sb.toString();
    }
}
