package io.tunproto.vertx;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.tunproto.vertx.TestSupport.await;
import static io.tunproto.vertx.TestSupport.listenEphemeral;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class TunnelServerAuthTest {

    private Vertx vertx;

    @BeforeEach
    void setup() {
        vertx = Vertx.vertx();
    }

    @AfterEach
    void teardown() throws Exception {
        await(vertx.close(), 5);
    }

    private boolean tryConnect(int port, String bearer) throws Exception {
        HttpClient client = vertx.createHttpClient();
        WebSocketConnectOptions opts = new WebSocketConnectOptions()
                .setHost("127.0.0.1").setPort(port).setURI("/tunnels");
        if (bearer != null) {
            opts.addHeader("Authorization", "Bearer " + bearer);
        }
        try {
            WebSocket ws = await(client.webSocket(opts), 5);
            await(ws.close(), 5);
            return true;
        } catch (Exception e) {
            return false;
        } finally {
            client.close();
        }
    }

    @Test
    void rejectsMissingAuthMode() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> TunnelServer.create(vertx, new TunnelOptions()));
        assertTrue(ex.getMessage().contains("no auth"));
    }

    @Test
    void rejectsMultipleAuthModes() {
        assertThrows(IllegalArgumentException.class,
                () -> TunnelServer.create(vertx, new TunnelOptions().addApiKey("k").setAuthDisabled(true)));
    }

    @Test
    void badKeyIsRejected() throws Exception {
        TunnelServer server = TunnelServer.create(vertx, new TunnelOptions().addApiKey("good-key"));
        int port = listenEphemeral(server, 5);
        assertTrue(tryConnect(port, "good-key"), "good key must upgrade");
        if (tryConnect(port, "WRONG")) {
            fail("bad key must be rejected");
        }
        await(server.close(), 5);
    }

    @Test
    void authDisabledAcceptsAny() throws Exception {
        TunnelServer server = TunnelServer.create(vertx, new TunnelOptions().setAuthDisabled(true));
        int port = listenEphemeral(server, 5);
        assertTrue(tryConnect(port, null), "authDisabled must accept no-header upgrade");
        await(server.close(), 5);
    }

    @Test
    void overMaxSessionsRejected() throws Exception {
        TunnelServer server = TunnelServer.create(vertx,
                new TunnelOptions().setAuthDisabled(true).setMaxSessions(1));
        int port = listenEphemeral(server, 5);

        HttpClient client = vertx.createHttpClient();
        WebSocketConnectOptions opts = new WebSocketConnectOptions()
                .setHost("127.0.0.1").setPort(port).setURI("/tunnels");
        WebSocket first = await(client.webSocket(opts), 5); // keep it open
        boolean secondOk;
        try {
            await(client.webSocket(opts), 5);
            secondOk = true;
        } catch (Exception e) {
            secondOk = false;
        }
        if (secondOk) {
            fail("second session over cap must be rejected");
        }
        await(first.close(), 5);
        client.close();
        await(server.close(), 5);
    }
}
