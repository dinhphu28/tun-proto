package io.tunproto.scala

import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.concurrent.Await
import scala.concurrent.duration._

class ConcurrentLargeSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {
  private implicit val system: ActorSystem = ActorSystem("cl-test")
  override def afterAll(): Unit = Await.ready(system.terminate(), 10.seconds)

  "concurrent large streams" should {
    "round-trip 6 x 512KiB through echo without a protocol error" in {
      val echo = new EchoTarget(); echo.start()
      val server = TunnelServer(TunnelOptions(apiKeys = Set("secret")))
      val binding = Await.result(server.listen("127.0.0.1", 0), 10.seconds)
      try {
        val url = s"ws://127.0.0.1:${binding.localAddress.getPort}/tunnels"
        val client = new TunnelTestClient(url, "secret")
        val n = 512 * 1024
        val streams = (0 until 6).map(_ => client.openStream("127.0.0.1", echo.port))
        val payloads = streams.map { s =>
          val p = new Array[Byte](n); new java.util.Random().nextBytes(p)
          val w = new Thread(() => s.write(p)); w.start()
          (s, p, w)
        }
        payloads.foreach { case (s, p, w) =>
          val got = s.readN(n, 30.seconds)
          w.join(30000)
          java.util.Arrays.equals(got, p) shouldBe true
        }
        client.close()
      } finally {
        echo.stop()
        Await.ready(server.close(), 5.seconds)
        Await.ready(binding.unbind(), 5.seconds)
      }
    }
  }
}
