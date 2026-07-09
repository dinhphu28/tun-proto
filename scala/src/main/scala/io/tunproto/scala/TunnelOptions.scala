package io.tunproto.scala

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/**
 * Configuration for a [[TunnelServer]]. All fields have defaults; provide exactly
 * one auth mode.
 *
 * ==Flow-control note (important — the default maxStreamWindow is 256 KiB)==
 *
 * The reused yamux-core replenishes a stream's receive window (the UP direction:
 * peer -> target) only once accumulated consumed bytes reach `maxStreamWindow / 2`
 * (see `YamuxStream.consumed`). But the INITIAL receive window is fixed at 256 KiB
 * (`YamuxConstants.INITIAL_WINDOW`) regardless of `maxStreamWindow`. If
 * `maxStreamWindow / 2 > 256 KiB`, the peer exhausts its 256 KiB window before the
 * server ever emits a WINDOW_UPDATE, and an UP transfer larger than 256 KiB
 * DEADLOCKS. We therefore default `maxStreamWindow` to 256 KiB so the replenish
 * threshold (128 KiB) sits below the initial window. The DOWN direction (target ->
 * peer) is governed by the send window + the SYN delta, which yamux-core applies
 * correctly, so large downloads are unaffected by this setting.
 *
 * You may raise `maxStreamWindow` for higher throughput, but only up to 512 KiB
 * (2 * INITIAL_WINDOW) while the core keeps the `maxStreamWindow / 2` threshold;
 * beyond that, UP transfers can stall. The [[require]] below enforces this.
 */
final case class TunnelOptions(
    // ---- auth (choose exactly one mode; validated) ----
    apiKeys: Set[String] = Set.empty,
    authDisabled: Boolean = false,
    authenticator: Option[String => Future[Boolean]] = None,

    // ---- egress ----
    allowTarget: Option[(String, Int) => Boolean] = None, // None = allow-all (+ warn once)

    // ---- endpoint ----
    path: String = "tunnels",

    // ---- yamux window / caps ----
    // 256 KiB. See the class doc: with the core's maxStreamWindow/2 replenish
    // threshold and a fixed 256 KiB initial window, this is the safe default that
    // keeps UP transfers > 256 KiB from deadlocking.
    maxStreamWindow: Int = 262144,
    maxConcurrentStreamsPerSession: Int = 0, // 0 = unlimited
    maxSessions: Int = 0,                    // 0 = unlimited (over -> 503)
    maxStreams: Int = 0,                     // 0 = unlimited global (over -> RST)

    // ---- timeouts ----
    dialTimeout: FiniteDuration = 10.seconds,
    keepAliveInterval: FiniteDuration = 30.seconds,
    keepAliveTimeout: FiniteDuration = 40.seconds,

    // ---- concurrency ----
    // Blocking EC for target socket IO. Default = virtual-thread-per-task.
    blockingEc: Option[ExecutionContext] = None
) {
  require(
    Seq(apiKeys.nonEmpty, authDisabled, authenticator.isDefined).count(identity) == 1,
    "Provide exactly one auth mode: apiKeys, authDisabled=true, or authenticator."
  )
  require(maxStreamWindow >= 262144, "maxStreamWindow must be >= 256 KiB (yamux floor).")
  require(
    maxStreamWindow <= 524288,
    "maxStreamWindow must be <= 512 KiB: yamux-core replenishes a stream's receive " +
      "window only at maxStreamWindow/2, but the initial window is fixed at 256 KiB, so a " +
      "larger cap makes UP transfers > 256 KiB deadlock. Verified against the real Go client."
  )
  require(path.nonEmpty, "path must be non-empty.")
}
