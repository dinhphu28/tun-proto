package io.tunproto.vertx;

import io.vertx.core.Future;
import io.vertx.core.Vertx;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Small blocking-await helpers for the test thread (never used on an event loop). */
final class TestSupport {

    private TestSupport() {
    }

    /** Block the (test) thread until the Vert.x Future completes; return its value. */
    static <T> T await(Future<T> f, long timeoutSec) throws Exception {
        CompletableFuture<T> cf = new CompletableFuture<>();
        f.onComplete(ar -> {
            if (ar.succeeded()) {
                cf.complete(ar.result());
            } else {
                cf.completeExceptionally(ar.cause());
            }
        });
        return cf.get(timeoutSec, TimeUnit.SECONDS);
    }

    /** Bind a TunnelServer on an ephemeral port and return the actual port. */
    static int listenEphemeral(TunnelServer server, long timeoutSec) throws Exception {
        await(server.listen(0), timeoutSec);
        return boundPort(server);
    }

    static int boundPort(TunnelServer server) {
        try {
            var field = server.getClass().getDeclaredField("ownedServer");
            field.setAccessible(true);
            io.vertx.core.http.HttpServer hs = (io.vertx.core.http.HttpServer) field.get(server);
            return hs.actualPort();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
