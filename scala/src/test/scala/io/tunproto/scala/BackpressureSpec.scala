package io.tunproto.scala

import akka.actor.ActorSystem
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.io.OutputStream
import java.net.{InetSocketAddress, ServerSocket, Socket}
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Backpressure + egress tests.
 *   - A slow-reading target that produces a large DOWN response exercises the
 *     writeQueueFull()/drainHandler park-and-release gate (Blocker 3): the transfer
 *     must complete byte-exact without a lost-wakeup hang.
 *   - Egress deny must RST the stream before dialing.
 */
class BackpressureSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  private implicit val system: ActorSystem = ActorSystem("bp-test")

  override def afterAll(): Unit = {
    Await.ready(system.terminate(), 10.seconds)
  }

  "DOWN backpressure" should {
    "deliver a large response through the drain gate without hanging" in {
      // Target that, on first byte received, blasts a large response then EOFs.
      val blaster = new BlasterTarget(payloadSize = 2 * 1024 * 1024)
      blaster.start()
      val server = TunnelServer(TunnelOptions(apiKeys = Set("secret")))
      val binding = Await.result(server.listen("127.0.0.1", 0), 10.seconds)
      try {
        val url = s"ws://127.0.0.1:${binding.localAddress.getPort}/tunnels"
        val client = new TunnelTestClient(url, "secret")
        val s = client.openStream("127.0.0.1", blaster.port)
        s.write(Array[Byte](1)) // trigger the blast
        val got = s.readN(blaster.payloadSize, 30.seconds)
        got.length shouldBe blaster.payloadSize
        java.util.Arrays.equals(got, blaster.payload) shouldBe true
        client.close()
      } finally {
        blaster.stop()
        Await.ready(server.close(), 5.seconds)
        Await.ready(binding.unbind(), 5.seconds)
      }
    }
  }

  "egress policy" should {
    "RST a stream to a denied target" in {
      val server = TunnelServer(
        TunnelOptions(apiKeys = Set("secret"), allowTarget = Some((_, port) => port == 1))
      )
      val binding = Await.result(server.listen("127.0.0.1", 0), 10.seconds)
      try {
        val url = s"ws://127.0.0.1:${binding.localAddress.getPort}/tunnels"
        val client = new TunnelTestClient(url, "secret")
        // Dial to port 9 (discard) which the policy denies (only port 1 allowed).
        val s = client.openStream("127.0.0.1", 9)
        // The server should RST -> our client surfaces EOF. readN returns empty.
        val got = s.readN(1, 5.seconds)
        got.length shouldBe 0
        client.close()
      } finally {
        Await.ready(server.close(), 5.seconds)
        Await.ready(binding.unbind(), 5.seconds)
      }
    }
  }
}

/** A target that, once it receives any byte, writes `payloadSize` bytes then EOFs. */
final class BlasterTarget(val payloadSize: Int) {
  private val server = new ServerSocket()
  @volatile private var running = true
  val payload: Array[Byte] = {
    val a = new Array[Byte](payloadSize)
    new java.util.Random(7).nextBytes(a)
    a
  }
  def port: Int = server.getLocalPort

  def start(): Unit = {
    server.bind(new InetSocketAddress("127.0.0.1", 0))
    val t = new Thread(() => {
      while (running) {
        try {
          val sock = server.accept()
          Thread.ofVirtual().start(() => pump(sock))
        } catch { case _: Throwable => running = false }
      }
    })
    t.setDaemon(true)
    t.start()
  }

  private def pump(sock: Socket): Unit = {
    try {
      val in = sock.getInputStream
      in.read() // wait for the trigger byte
      val out: OutputStream = sock.getOutputStream
      out.write(payload)
      out.flush()
      sock.shutdownOutput()
      // Drain the rest of the input so the peer's FIN is observed cleanly.
      val buf = new Array[Byte](8192)
      while (in.read(buf) >= 0) {}
    } catch { case _: Throwable => () }
    finally try sock.close() catch { case _: Throwable => () }
  }

  def stop(): Unit = { running = false; try server.close() catch { case _: Throwable => () } }
}
