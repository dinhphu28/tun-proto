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

class BackpressureTest {

    private Vertx vertx;

    @BeforeEach
    void setup() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void teardown() throws Exception {
        await(vertx.close(), 5);
    }

    /**
     * Target that echoes but reads slowly (pauses after each chunk, resumes on a
     * short timer). This forces the splicer's UP-direction {@code stream.consumed(n)}
     * to be throttled to the target's drain speed (the OOM guard): the WINDOW_UPDATE
     * replenish is emitted only as the slow target accepts bytes. The payload fits
     * within one initial window so it flows without needing a mid-stream replenish,
     * and it must round-trip intact despite the throttled drain.
     */
    @Test
    void slowTargetThrottlesButPreservesData() throws Exception {
        NetServer slow = vertx.createNetServer();
        slow.connectHandler(sock -> sock.handler(buf -> {
            sock.write(buf);
            sock.pause();
            vertx.setTimer(2, id -> sock.resume());
        }));
        int echoPort = await(slow.listen(0, "127.0.0.1"), 5).actualPort();

        TunnelServer server = TunnelServer.create(vertx, new TunnelOptions().setAuthDisabled(true));
        int tunnelPort = listenEphemeral(server, 5);

        YamuxTestClient client = new YamuxTestClient(vertx);
        await(client.connect(tunnelPort, "/tunnels", null), 5);

        int size = 200 * 1024; // within the 256 KiB initial window
        String payload = EndToEndSpliceTest.buildPayload(size, 'A');

        long sid = client.openStream("127.0.0.1", echoPort);
        YamuxTestClient.StreamState st = client.stream(sid);
        var done = st.awaitBytes(size);
        client.sendString(sid, payload);

        done.get(60, TimeUnit.SECONDS);
        assertFalse(st.rst, "stream must not be RST under backpressure");
        assertEquals(size, st.data.length(), "all bytes preserved despite slow target");
        assertEquals(payload, st.data.toString());

        await(client.close(), 5);
        await(server.close(), 5);
    }
}
