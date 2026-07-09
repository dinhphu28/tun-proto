package io.tunproto.vertx;

/**
 * Egress allow-policy (SSRF guard). Pure, synchronous, cheap: given the parsed
 * {@code host} and {@code port} of an OpenRequest target, return {@code true} to
 * permit dialing it or {@code false} to RST the stream.
 *
 * <p>If none is configured, the default allows everything and logs a one-time
 * SSRF warning. Configuring a policy is strongly recommended in production.</p>
 */
@FunctionalInterface
public interface TargetPolicy {
    boolean allow(String host, int port);
}
