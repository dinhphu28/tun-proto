package io.tunproto.scala

import scala.concurrent.Future

/**
 * Type aliases for the pluggable auth and egress hooks. Kept as plain function
 * types ("least learning") so callers just pass lambdas — no interfaces to
 * implement.
 */
object Authenticator {

  /** Given the Bearer token, decide (async) whether the connection is allowed. */
  type Authenticator = String => Future[Boolean]

  /** Given (host, port), decide whether the tunnel may dial the target. */
  type TargetPolicy = (String, Int) => Boolean
}
