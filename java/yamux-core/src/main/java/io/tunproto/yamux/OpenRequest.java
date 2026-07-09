package io.tunproto.yamux;

/**
 * Parsed OpenRequest (SPEC §5). Address parsing mirrors node/src/address.js:
 * bracketed [IPv6]:port else lastIndexOf(':').
 */
public record OpenRequest(String network, String address) {

    /** Host portion of {@link #address()}. Rejects empty host. */
    public String host() {
        return parse()[0];
    }

    /** Port portion of {@link #address()}. Rejects port &lt; 1 or &gt; 65535. */
    public int port() {
        return Integer.parseInt(parse()[1]);
    }

    private String[] parse() {
        String a = address;
        if (a == null || a.isEmpty()) {
            throw new ProtocolException("empty target address", YamuxConstants.GOAWAY_PROTO);
        }
        String host;
        String portStr;
        if (a.charAt(0) == '[') {
            int end = a.indexOf(']');
            if (end == -1) {
                throw new ProtocolException("invalid IPv6 address: " + a, YamuxConstants.GOAWAY_PROTO);
            }
            host = a.substring(1, end);
            if (end + 1 >= a.length() || a.charAt(end + 1) != ':') {
                throw new ProtocolException("missing port: " + a, YamuxConstants.GOAWAY_PROTO);
            }
            portStr = a.substring(end + 2);
        } else {
            int i = a.lastIndexOf(':');
            if (i == -1) {
                throw new ProtocolException("missing port: " + a, YamuxConstants.GOAWAY_PROTO);
            }
            host = a.substring(0, i);
            portStr = a.substring(i + 1);
        }
        if (host.isEmpty()) {
            throw new ProtocolException("empty host: " + a, YamuxConstants.GOAWAY_PROTO);
        }
        int port;
        try {
            port = Integer.parseInt(portStr);
        } catch (NumberFormatException e) {
            throw new ProtocolException("invalid port: " + portStr, YamuxConstants.GOAWAY_PROTO);
        }
        if (port < 1 || port > 65535) {
            throw new ProtocolException("invalid port: " + portStr, YamuxConstants.GOAWAY_PROTO);
        }
        return new String[] { host, Integer.toString(port) };
    }
}
