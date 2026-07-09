package io.tunproto.yamux;

/**
 * Callback for a newly accepted inbound stream (odd SYN). The core has NOT yet
 * sent ACK; the first outbound WINDOW_UPDATE/DATA for the stream piggybacks it.
 * The glue reads the OpenRequest, dials the target, and splices bytes.
 */
public interface StreamHandler {
    void onStream(YamuxStream stream);
}
