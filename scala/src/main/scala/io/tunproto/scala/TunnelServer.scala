package io.tunproto.scala

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route

import scala.concurrent.Future

/**
 * An embeddable reverse-TCP tunnel server speaking the tun-proto wire protocol.
 * Hides yamux, the WebSocket upgrade, and the OpenRequest framing entirely.
 *
 * {{{
 *   implicit val system: ActorSystem = ActorSystem("app")
 *   val server = TunnelServer(TunnelOptions(apiKeys = Set("secret")))
 *   val binding: Future[Http.ServerBinding] = server.listen("0.0.0.0", 8080)
 *   // OR embed into an existing Akka HTTP app:  val route: Route = server.route
 * }}}
 */
trait TunnelServer {

  /** The Akka HTTP route; mount into an existing app to share your port. */
  def route: Route

  /** Bind an owned HTTP server at host:port serving only [[route]]. */
  def listen(host: String, port: Int): Future[Http.ServerBinding]

  /** Live gauge: number of active tunnel sessions (WebSocket connections). */
  def activeSessions: Int

  /** Live gauge: number of active proxied streams across all sessions. */
  def activeStreams: Int

  /**
   * Stop keepalive scheduling and gracefully tear down owned resources (GO_AWAY +
   * close every live session). Does NOT shut down the caller's ActorSystem or an
   * externally provided blocking ExecutionContext.
   */
  def close(): Future[Unit]
}

object TunnelServer {

  /** Create a tunnel server bound to the given ActorSystem. */
  def apply(options: TunnelOptions)(implicit system: ActorSystem): TunnelServer =
    new internal.TunnelServerImpl(options)
}
