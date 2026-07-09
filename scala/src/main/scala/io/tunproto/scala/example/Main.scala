package io.tunproto.scala.example

import akka.actor.ActorSystem
import io.tunproto.scala.{TunnelOptions, TunnelServer}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Minimal embedded example. Binds ws://0.0.0.0:8080/tunnels accepting Bearer
 * "secret". Point the Go client at it with:
 *   {"server_url":"ws://127.0.0.1:8080/tunnels","api_key":"secret",
 *    "forwards":[{"local":"127.0.0.1:15432","remote":"127.0.0.1:5432"}]}
 */
object Main {
  def main(args: Array[String]): Unit = {
    implicit val system: ActorSystem = ActorSystem("tun-proto-example")
    import system.dispatcher

    val server = TunnelServer(TunnelOptions(apiKeys = Set("secret")))
    server.listen("0.0.0.0", 8080).onComplete {
      case Success(binding) =>
        system.log.info(s"tunnel listening on ws://${binding.localAddress}/tunnels")
      case Failure(e) =>
        system.log.error(e, "failed to bind")
        system.terminate()
    }

    // Keep alive until interrupted.
    Await.result(system.whenTerminated, Duration.Inf)
  }
}
