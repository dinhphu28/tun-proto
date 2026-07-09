package io.tunproto.scala.example

import akka.actor.ActorSystem
import io.tunproto.scala.{TunnelOptions, TunnelServer}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
 * Configurable runner (mainly for interop tests): args = port apiKey.
 * Binds ws://127.0.0.1:<port>/tunnels and permits only 127.0.0.1 targets.
 * Prints "SCALA-TUNNEL-READY <port>" once bound.
 */
object Runner {
  def main(args: Array[String]): Unit = {
    val port = args(0).toInt
    val key = args(1)
    implicit val system: ActorSystem = ActorSystem("tun-proto-runner")
    import system.dispatcher

    val server = TunnelServer(
      TunnelOptions(
        apiKeys = Set(key),
        allowTarget = Some((host, _) => host == "127.0.0.1")
      )
    )
    server.listen("127.0.0.1", port).onComplete {
      case Success(_) => println("SCALA-TUNNEL-READY " + port)
      case Failure(e) => System.err.println("bind failed: " + e); sys.exit(1)
    }
    Await.result(system.whenTerminated, Duration.Inf)
  }
}
