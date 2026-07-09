/**
 * Framework-agnostic, sans-IO yamux v0 SERVER engine plus OpenRequest codec and
 * flow control. No blocking IO, no framework dependencies (only netty-buffer +
 * jackson-databind).
 *
 * <h2>Threading</h2>
 * Single-threaded by contract: all of {@code receive/write/consumed/tick/
 * sendPing/close} for one {@link io.tunproto.yamux.YamuxSession} run on the same
 * thread (the connection's event loop). No locks.
 *
 * <h2>Buffer-ownership contract (canonical)</h2>
 * <table border="1">
 *   <caption>Ownership</caption>
 *   <tr><th>API</th><th>Ownership</th></tr>
 *   <tr><td>{@code session.receive(buf)}</td>
 *       <td>Caller keeps ownership; session COPIES what it needs into its own
 *           heap accumulator and never releases {@code buf}.</td></tr>
 *   <tr><td>{@code OutboundHandler.onFrame(frame)}</td>
 *       <td>Ownership -&gt; handler; handler MUST write, then release AFTER the
 *           write completes (never synchronously).</td></tr>
 *   <tr><td>{@code stream.dataHandler} payload</td>
 *       <td>Retained slice -&gt; handler; handler MUST {@code release()} after
 *           consuming.</td></tr>
 *   <tr><td>{@code stream.write(data)}</td>
 *       <td>Ownership -&gt; stream; stream releases when fully sent.</td></tr>
 *   <tr><td>{@code OpenRequestReader.offer(chunk)}</td>
 *       <td>Ownership -&gt; reader (copies, then releases chunk). The returned
 *           {@code Result.leftover} is OWNED by the caller.</td></tr>
 * </table>
 *
 * <h2>Key interop lessons</h2>
 * <ol>
 *   <li>APPLY THE SYN WINDOW DELTA: a SYN-flagged WINDOW_UPDATE both creates the
 *       stream and bumps its SEND window. Dropping it deadlocks bulk downloads at
 *       exactly 262144 bytes.</li>
 *   <li>REPLENISH AS RECEIVER: as the app consumes received DATA, send
 *       WINDOW_UPDATE (delta &ge; maxWindow/2 or when carrying ACK).</li>
 *   <li>ACK on first outbound frame for an accepted stream.</li>
 *   <li>FIN/RST are framed as WINDOW_UPDATE (type=1) delta 0, NOT DATA.</li>
 * </ol>
 */
package io.tunproto.yamux;
