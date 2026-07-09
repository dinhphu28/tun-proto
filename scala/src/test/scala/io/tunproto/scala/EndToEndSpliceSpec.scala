package io.tunproto.scala

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.wordspec.AnyWordSpec

import java.io.{InputStream, OutputStream}
import java.net.{InetSocketAddress, ServerSocket, Socket}
import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * End-to-end splice tests against a real bound TunnelServer and a real echo target
 * socket, driven by [[TunnelTestClient]] speaking yamux over a real WebSocket.
 *
 * Proves the interop-critical paths:
 *   - small round trip,
 *   - a > 256 KiB DOWN transfer (target -> peer) does not stall at 262144
 *     (SYN send-window delta path),
 *   - a > 256 KiB UP transfer (peer -> target) does not deadlock (the
 *     consumed()/replenish path; Blocker 1),
 *   - concurrent multiplexed streams.
 */
class EndToEndSpliceSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll
    with ScalaFutures {

  implicit override val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = Span(20, Seconds), interval = Span(50, Millis))

  private implicit val system: ActorSystem = ActorSystem("e2e-test")

  private var echo: EchoTarget = _
  private var server: TunnelServer = _
  private var binding: Http.ServerBinding = _
  private var wsUrl: String = _

  override def beforeAll(): Unit = {
    echo = new EchoTarget()
    echo.start()
    server = TunnelServer(TunnelOptions(apiKeys = Set("secret")))
    binding = Await.result(server.listen("127.0.0.1", 0), 10.seconds)
    val port = binding.localAddress.getPort
    wsUrl = s"ws://127.0.0.1:$port/tunnels"
  }

  override def afterAll(): Unit = {
    try echo.stop() catch { case _: Throwable => () }
    Await.ready(server.close(), 5.seconds)
    Await.ready(binding.unbind(), 5.seconds)
    Await.ready(system.terminate(), 10.seconds)
  }

  private def echoAddr: (String, Int) = ("127.0.0.1", echo.port)

  "TunnelServer splice" should {

    "round-trip a small payload through an echo target" in {
      val client = new TunnelTestClient(wsUrl, "secret")
      val (h, p) = echoAddr
      val s = client.openStream(h, p)
      val msg = "hello tunnel".getBytes("UTF-8")
      s.write(msg)
      val got = s.readN(msg.length, 10.seconds)
      new String(got, "UTF-8") shouldBe "hello tunnel"
      s.fin()
      client.close()
    }

    "download > 256 KiB (DOWN) without stalling at 262144" in {
      // The echo target sends back whatever we send. We push a 1 MiB payload up and
      // read 1 MiB back down. The DOWN read exercises the send-window/SYN-delta
      // path (the classic 262144 stall).
      val client = new TunnelTestClient(wsUrl, "secret")
      val (h, p) = echoAddr
      val s = client.openStream(h, p)
      val n = 1024 * 1024
      val payload = randomBytes(n)
      // Write on a separate thread so UP and DOWN proceed concurrently (avoid
      // self-deadlock if the target's socket buffers fill).
      val writer = new Thread(() => s.write(payload))
      writer.start()
      val got = s.readN(n, 20.seconds)
      writer.join(20000)
      got.length shouldBe n
      java.util.Arrays.equals(got, payload) shouldBe true
      s.fin()
      client.close()
    }

    "upload > 256 KiB (UP) to the target without deadlocking (Blocker 1)" in {
      // A sink target that reads everything and reports the total. Proves the
      // server replenishes recvWindow via consumed() so an UP transfer larger than
      // the 256 KiB initial window proceeds.
      val sink = new SinkTarget()
      sink.start()
      try {
        val server2 = TunnelServer(TunnelOptions(apiKeys = Set("secret")))
        val b2 = Await.result(server2.listen("127.0.0.1", 0), 10.seconds)
        try {
          val url2 = s"ws://127.0.0.1:${b2.localAddress.getPort}/tunnels"
          val client = new TunnelTestClient(url2, "secret")
          val s = client.openStream("127.0.0.1", sink.port)
          val n = 1024 * 1024 // 1 MiB >> 256 KiB
          val payload = randomBytes(n)
          s.write(payload) // must NOT block forever
          s.fin()
          sink.awaitTotal(n, 20.seconds) shouldBe n
          client.close()
        } finally {
          Await.ready(server2.close(), 5.seconds)
          Await.ready(b2.unbind(), 5.seconds)
        }
      } finally sink.stop()
    }

    "handle concurrent multiplexed streams" in {
      val client = new TunnelTestClient(wsUrl, "secret")
      val (h, p) = echoAddr
      val results = (0 until 8).map { i =>
        val s = client.openStream(h, p)
        val msg = s"stream-$i-payload".getBytes("UTF-8")
        s.write(msg)
        (s, msg)
      }
      results.foreach { case (s, msg) =>
        val got = s.readN(msg.length, 10.seconds)
        java.util.Arrays.equals(got, msg) shouldBe true
        s.fin()
      }
      client.close()
    }
  }

  private def randomBytes(n: Int): Array[Byte] = {
    val a = new Array[Byte](n)
    new java.util.Random(42).nextBytes(a)
    a
  }
}

/** A blocking echo TCP server: echoes every byte back. */
final class EchoTarget {
  private val server = new ServerSocket()
  @volatile private var running = true
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
      val in: InputStream = sock.getInputStream
      val out: OutputStream = sock.getOutputStream
      val buf = new Array[Byte](32 * 1024)
      var read = in.read(buf)
      while (read >= 0) {
        out.write(buf, 0, read)
        out.flush()
        read = in.read(buf)
      }
    } catch { case _: Throwable => () }
    finally try sock.close() catch { case _: Throwable => () }
  }

  def stop(): Unit = { running = false; try server.close() catch { case _: Throwable => () } }
}

/** A sink TCP server: reads all bytes and counts them. */
final class SinkTarget {
  private val server = new ServerSocket()
  @volatile private var running = true
  private val total = new java.util.concurrent.atomic.AtomicLong(0)
  private val lock = new Object()
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
      val buf = new Array[Byte](32 * 1024)
      var read = in.read(buf)
      while (read >= 0) {
        lock.synchronized { total.addAndGet(read); lock.notifyAll() }
        read = in.read(buf)
      }
    } catch { case _: Throwable => () }
    finally try sock.close() catch { case _: Throwable => () }
  }

  def awaitTotal(n: Int, timeout: FiniteDuration): Long = {
    val deadline = System.nanoTime() + timeout.toNanos
    lock.synchronized {
      while (total.get() < n && System.nanoTime() < deadline) {
        val remMs = math.max(1, (deadline - System.nanoTime()) / 1000000)
        lock.wait(remMs)
      }
    }
    total.get()
  }

  def stop(): Unit = { running = false; try server.close() catch { case _: Throwable => () } }
}
