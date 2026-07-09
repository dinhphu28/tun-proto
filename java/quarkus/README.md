# tun-proto-quarkus

Embed a **reverse-TCP tunnel server** in your Quarkus app. A tun-proto client
(e.g. the Go `tunnel-client`) connects over WebSocket and forwards TCP
connections to targets your server can reach — useful for exposing internal
services (a database, an internal API) through an app you already run.

**You need to know nothing about yamux, WebSocket upgrades, or the OpenRequest
wire format.** Add the dependency, set an API key, done. The tunnel shares your
app's existing HTTP port and lifecycle; only WebSocket upgrades on
`tun-proto.path` (`/tunnels`) are claimed — all your normal routes are untouched.

## 1. Add the dependency

```xml
<dependency>
  <groupId>io.tunproto</groupId>
  <artifactId>tun-proto-quarkus</artifactId>
  <version>1.0.0</version>
</dependency>
```

## 2. Set an API key

In your app's `src/main/resources/application.properties`:

```properties
tun-proto.api-keys[0]=my-secret-key
```

That is the entire integration. On startup you'll see:

```
tun-proto: tunnel mounted on the app HTTP server at path /tunnels (shares the app port; WebSocket upgrades only)
```

A client then connects to `ws://<your-app-host>:<your-app-port>/tunnels` with
header `Authorization: Bearer my-secret-key`.

### Go client example

```json
{
  "server_url": "ws://127.0.0.1:8080/tunnels",
  "api_key": "my-secret-key",
  "forwards": [{ "local": "127.0.0.1:15432", "remote": "127.0.0.1:5432" }]
}
```

```bash
tunnel-client -config config.json
```

## Configuration reference

All properties are prefixed `tun-proto.`:

| Property | Default | Meaning |
|---|---|---|
| `enabled` | `true` | Master switch. |
| `path` | `/tunnels` | WebSocket upgrade path. |
| `port` | *(unset)* | Unset = mount on the app's HTTP server (share the port). Set = bind an own listener. |
| `api-keys[n]` | *(empty)* | Static Bearer allowlist. |
| `auth-disabled` | `false` | Dev only: accept every upgrade. |
| `max-stream-window` | `16777216` | Per-stream yamux window (matches the Go client). |
| `max-sessions` | `0` | Max concurrent WebSocket sessions (0 = unlimited; over-limit ⇒ 503). |
| `max-streams` | `0` | Max concurrent proxied streams across all sessions (0 = unlimited; over-limit ⇒ RST). |
| `dial-timeout` | `10s` | Target connect timeout. |
| `keep-alive-interval` | `30s` | PING interval per session. |
| `keep-alive-timeout` | `40s` | Session torn down if no PONG within this. |

> **Exactly one auth mode** must be active: a non-empty `api-keys`, an
> `Authenticator` CDI bean, **or** `auth-disabled=true`. If none is set the app
> fails fast at startup with a clear message.

## Own port instead of sharing (opt-in)

For a separate ingress/security zone or independent scaling:

```properties
tun-proto.port=9090
```

Now the tunnel owns its own `HttpServer` on `9090`, closed cleanly on shutdown.

## Locking down egress (recommended for production)

By default any target `(host, port)` is allowed and a one-time SSRF warning is
logged. Restrict it by defining an `@ApplicationScoped` `TargetPolicy` bean —
it is auto-wired, no config needed:

```java
import io.tunproto.vertx.TargetPolicy;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class MyEgressPolicy implements TargetPolicy {
    @Override
    public boolean allow(String host, int port) {
        return host.equals("10.0.0.5") && port == 5432; // only the DB
    }
}
```

## Custom async authentication (optional)

Define an `@ApplicationScoped` `Authenticator` bean to validate keys against
your own store (non-blocking; return a `Future<Boolean>`). It overrides the
static `api-keys` allowlist:

```java
import io.tunproto.vertx.Authenticator;
import io.tunproto.vertx.AuthContext;
import io.vertx.core.Future;
import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class MyAuthenticator implements Authenticator {
    @Override
    public Future<Boolean> authenticate(String bearerKey, AuthContext ctx) {
        return Future.succeededFuture(myKeyStore.isValid(bearerKey));
    }
}
```

## Metrics / lifecycle

Inject the running server for live gauges:

```java
@Inject io.tunproto.vertx.TunnelServer tunnel;
// tunnel.activeSessions(); tunnel.activeStreams();
// tunnel.onStream(ev -> ...); tunnel.onStreamError(ev -> ...);
```

## How this differs from `tun-proto-vertx`

This module adds **only** bootstrap + CDI + config. The engine (WebSocket
handler, async `NetClient` dialing, the backpressured yamux splice, and every
use of `yamux-core`) is 100% reused from `tun-proto-vertx`. There is zero
protocol logic here — just a `@ConfigMapping`, an `@ApplicationScoped` starter
bean that observes `StartupEvent`/`ShutdownEvent`, and a `@Produces` for the
`TunnelServer`.
