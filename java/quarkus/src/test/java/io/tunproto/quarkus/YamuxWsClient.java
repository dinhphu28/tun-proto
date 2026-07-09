package io.tunproto.quarkus;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.WebSocket;
import io.vertx.core.http.WebSocketConnectOptions;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Minimal in-JVM yamux CLIENT over a binary WebSocket, used only to drive the
 * mounted tunnel server in the @QuarkusTest. Hand-frames yamux (the server
 * engine never opens streams), sends the length-prefixed OpenRequest, and
 * reassembles inbound DATA per stream. Mirrors the vertx module's test client.
 */
final class YamuxWsClient {

    private static final byte VERSION = 0;
    private static final int T_DATA = 0;
    private static final int T_WINDOW_UPDATE = 1;
    private static final int T_PING = 2;

    private static final int F_SYN = 1;
    private static final int F_ACK = 2;
    private static final int F_FIN = 4;
    private static final int F_RST = 8;

    private static final int INITIAL_WINDOW = 262_144;

    private final Vertx vertx;
    private HttpClient httpClient;
    private WebSocket ws;

    private long nextStreamId = 1; // odd ids
    private Buffer accum = Buffer.buffer();

    private final Map<Long, StreamState> streams = new ConcurrentHashMap<>();

    static final class StreamState {
        final StringBuilder data = new StringBuilder();
        final CompletableFuture<Void> finished = new CompletableFuture<>();
        volatile boolean rst = false;
        volatile int received = 0;

        private int awaitBytes = -1;
        private CompletableFuture<Void> reachedBytes;

        synchronized CompletableFuture<Void> awaitBytes(int n) {
            this.awaitBytes = n;
            this.reachedBytes = new CompletableFuture<>();
            if (received >= n) {
                reachedBytes.complete(null);
            }
            return reachedBytes;
        }

        synchronized void onReceived() {
            if (reachedBytes != null && received >= awaitBytes && !reachedBytes.isDone()) {
                reachedBytes.complete(null);
            }
        }
    }

    YamuxWsClient(Vertx vertx) {
        this.vertx = vertx;
    }

    Future<Void> connect(int port, String path, String bearerKey) {
        this.httpClient = vertx.createHttpClient();
        WebSocketConnectOptions opts = new WebSocketConnectOptions()
                .setHost("127.0.0.1").setPort(port).setURI(path);
        if (bearerKey != null) {
            opts.addHeader("Authorization", "Bearer " + bearerKey);
        }
        Promise<Void> p = Promise.promise();
        httpClient.webSocket(opts).onComplete(ar -> {
            if (ar.failed()) {
                p.fail(ar.cause());
                return;
            }
            this.ws = ar.result();
            ws.handler(this::onInbound);
            p.complete();
        });
        return p.future();
    }

    long openStream(String targetHost, int targetPort) {
        long id = nextStreamId;
        nextStreamId += 2;
        streams.put(id, new StreamState());

        int delta = 16 * 1024 * 1024 - INITIAL_WINDOW;
        writeFrame(header(T_WINDOW_UPDATE, F_SYN, id, delta));

        String json = "{\"network\":\"tcp\",\"address\":\"" + targetHost + ":" + targetPort + "\"}";
        byte[] jb = json.getBytes(StandardCharsets.UTF_8);
        Buffer or = Buffer.buffer();
        or.appendInt(jb.length);
        or.appendBytes(jb);
        sendData(id, or);
        return id;
    }

    void sendData(long id, Buffer payload) {
        int off = 0;
        int total = payload.length();
        while (off < total) {
            int n = Math.min(65_536, total - off);
            Buffer chunk = payload.getBuffer(off, off + n);
            Buffer frame = Buffer.buffer();
            frame.appendBytes(rawHeader(T_DATA, 0, id, n));
            frame.appendBuffer(chunk);
            writeFrame(frame);
            off += n;
        }
    }

    void sendString(long id, String s) {
        sendData(id, Buffer.buffer(s.getBytes(StandardCharsets.UTF_8)));
    }

    StreamState stream(long id) {
        return streams.get(id);
    }

    Future<Void> close() {
        Promise<Void> p = Promise.promise();
        if (ws != null) {
            ws.close().onComplete(ar -> {
                if (httpClient != null) {
                    httpClient.close().onComplete(x -> p.complete());
                } else {
                    p.complete();
                }
            });
        } else {
            p.complete();
        }
        return p.future();
    }

    // ---- framing ----

    private Buffer header(int type, int flags, long streamId, long length) {
        return Buffer.buffer(rawHeader(type, flags, streamId, length));
    }

    private byte[] rawHeader(int type, int flags, long streamId, long length) {
        byte[] h = new byte[12];
        h[0] = VERSION;
        h[1] = (byte) type;
        h[2] = (byte) ((flags >> 8) & 0xFF);
        h[3] = (byte) (flags & 0xFF);
        h[4] = (byte) ((streamId >> 24) & 0xFF);
        h[5] = (byte) ((streamId >> 16) & 0xFF);
        h[6] = (byte) ((streamId >> 8) & 0xFF);
        h[7] = (byte) (streamId & 0xFF);
        h[8] = (byte) ((length >> 24) & 0xFF);
        h[9] = (byte) ((length >> 16) & 0xFF);
        h[10] = (byte) ((length >> 8) & 0xFF);
        h[11] = (byte) (length & 0xFF);
        return h;
    }

    private void writeFrame(Buffer frame) {
        ws.writeBinaryMessage(frame);
    }

    // ---- inbound parse (server -> client) ----

    private void onInbound(Buffer buf) {
        accum.appendBuffer(buf);
        int ri = 0;
        while (accum.length() - ri >= 12) {
            int type = accum.getByte(ri + 1) & 0xFF;
            int flags = ((accum.getByte(ri + 2) & 0xFF) << 8) | (accum.getByte(ri + 3) & 0xFF);
            long sid = accum.getUnsignedInt(ri + 4);
            long len = accum.getUnsignedInt(ri + 8);

            if (type == T_DATA) {
                if (accum.length() - ri < 12 + len) {
                    break;
                }
                if (len > 0) {
                    Buffer payload = accum.getBuffer(ri + 12, (int) (ri + 12 + len));
                    StreamState st = streams.computeIfAbsent(sid, k -> new StreamState());
                    st.data.append(payload.toString(StandardCharsets.UTF_8));
                    st.received += (int) len;
                    st.onReceived();
                    writeFrame(header(T_WINDOW_UPDATE, 0, sid, len));
                }
                if ((flags & F_FIN) != 0) {
                    finish(sid, false);
                }
                if ((flags & F_RST) != 0) {
                    finish(sid, true);
                }
                ri += 12 + (int) len;
            } else {
                if (type == T_PING && (flags & F_SYN) != 0) {
                    writeFrame(header(T_PING, F_ACK, 0, len));
                }
                if (type == T_WINDOW_UPDATE) {
                    if ((flags & F_FIN) != 0) {
                        finish(sid, false);
                    }
                    if ((flags & F_RST) != 0) {
                        finish(sid, true);
                    }
                }
                ri += 12;
            }
        }
        accum = accum.getBuffer(ri, accum.length());
    }

    private void finish(long sid, boolean rst) {
        StreamState st = streams.computeIfAbsent(sid, k -> new StreamState());
        st.rst = rst;
        st.finished.complete(null);
    }
}
