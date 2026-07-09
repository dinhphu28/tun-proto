package io.tunproto.scala

/**
 * Internal, library-level errors used on the splice/auth/dial paths. These never
 * leak yamux types to callers; they are only surfaced through logs and the
 * (optional) error side channels.
 */
sealed abstract class TunnelException(message: String, cause: Throwable)
    extends RuntimeException(message, cause)

object TunnelException {

  final class AuthException(message: String)
      extends TunnelException(message, null)

  /** Egress policy denied the requested target. */
  final class EgressDeniedException(host: String, port: Int)
      extends TunnelException(s"egress denied by allowTarget: $host:$port", null)

  /** Failed to dial the requested target. */
  final class DialException(host: String, port: Int, cause: Throwable)
      extends TunnelException(s"failed to dial target $host:$port", cause)
}
