package io.tunproto.scala

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, WebSocketRequest}
import akka.stream.scaladsl.{Flow, Sink, Source, SourceQueueWithComplete}
import akka.stream.{Materializer, OverflowStrategy}
import akka.util.ByteString

import java.nio.charset.StandardCharsets
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * A minimal, blocking yamux CLIENT for tests. Connects a real WebSocket to a
 * running TunnelServer and drives the tun-proto wire protocol as the Go client
 * would: opens odd streams with SYN + a large window delta, sends the
 * length-prefixed OpenRequest JSON, then reads/writes DATA. It replenishes the
 * peer's send window as a receiver so DOWN transfers do not stall, and consumes
 * WINDOW_UPDATE as a sender so UP transfers can proceed.
 *
 * Single background thread drains inbound WS bytes and parses frames; per-stream
 * inbound bytes land in a blocking queue that tests read from.
 */
final class TunnelTestClient(url: String, apiKey: String)(implicit system: ActorSystem) {

  private implicit val ec: ExecutionContext = system.dispatcher
  private implicit val mat: Materializer = Materializer(system)

  private val VERSION: Byte = 0
  private val HEADER = 12
  private val T_DATA = 0
  private val T_WINDOW_UPDATE = 1
  private val T_PING = 2
  private val T_GOAWAY = 3
  private val F_SYN = 1
  private val F_ACK = 2
  private val F_FIN = 4
  private val F_RST = 8
  private val INITIAL_WINDOW = 262144
  private val CLIENT_WINDOW = 16 * 1024 * 1024 // 16 MiB, like the Go client

  private val nextId = new AtomicLong(1) // odd stream ids

  // Outbound WS queue.
  private val outQueue: SourceQueueWithComplete[Message] = {
    val (q, src) = Source.queue[Message](1024, OverflowStrategy.backpressure).preMaterialize()
    // Coalesce any streamed frames to strict on the stream (async), then feed the
    // parser from a single-threaded sink so `accum`/`parseLoop` stay single-threaded.
    val inSink: Sink[Message, _] =
      Flow[Message]
        .mapAsync(1) { (m: Message) =>
          m match {
            case bm: BinaryMessage => bm.toStrict(5.seconds).map(s => Some(s.data): Option[ByteString])
            case _                 => Future.successful(None: Option[ByteString])
          }
        }
        .collect { case Some(d) => d }
        .to(Sink.foreach[ByteString](onWsBytes))
    val flow: Flow[Message, Message, _] = Flow.fromSinkAndSource(inSink, src)
    val (upgradeResp, _) = Http().singleWebSocketRequest(
      WebSocketRequest(url).copy(extraHeaders =
        collection.immutable.Seq(Authorization(OAuth2BearerToken(apiKey)))
      ),
      flow
    )
    Await.result(upgradeResp, 10.seconds)
    q
  }

  // Inbound parse accumulator (only touched by the WS sink thread).
  @volatile private var accum: ByteString = ByteString.empty

  final class Stream(val id: Long) {
    val inbound = new LinkedBlockingQueue[Either[Unit, Array[Byte]]]() // Right=data, Left=EOF
    @volatile var sendWindow: Long = INITIAL_WINDOW.toLong
    @volatile var recvConsumed: Int = 0
    val windowGate = new Object()

    /** Read up to `min` bytes total (blocking), or until EOF. Returns collected bytes. */
    def readN(n: Int, timeout: FiniteDuration): Array[Byte] = {
      val out = new java.io.ByteArrayOutputStream()
      val deadline = System.nanoTime() + timeout.toNanos
      while (out.size() < n) {
        val remaining = deadline - System.nanoTime()
        if (remaining <= 0) throw new RuntimeException(s"readN timeout, got ${out.size()}/$n")
        inbound.poll(remaining, TimeUnit.NANOSECONDS) match {
          case null                => throw new RuntimeException(s"readN timeout, got ${out.size()}/$n")
          case Left(_)             => return out.toByteArray // EOF
          case Right(d)            => out.write(d); replenish(d.length)
        }
      }
      out.toByteArray
    }

    /** Drain until EOF, returning everything. */
    def readUntilEof(timeout: FiniteDuration): Array[Byte] = {
      val out = new java.io.ByteArrayOutputStream()
      val deadline = System.nanoTime() + timeout.toNanos
      var eof = false
      while (!eof) {
        val remaining = deadline - System.nanoTime()
        if (remaining <= 0) throw new RuntimeException(s"readUntilEof timeout, got ${out.size()}")
        inbound.poll(remaining, TimeUnit.NANOSECONDS) match {
          case null    => throw new RuntimeException(s"readUntilEof timeout, got ${out.size()}")
          case Left(_) => eof = true
          case Right(d) => out.write(d); replenish(d.length)
        }
      }
      out.toByteArray
    }

    private def replenish(n: Int): Unit = {
      recvConsumed += n
      if (recvConsumed >= CLIENT_WINDOW / 2) {
        val delta = recvConsumed
        recvConsumed = 0
        sendFrame(T_WINDOW_UPDATE, 0, id, delta, null)
      }
    }

    /** Write bytes to the peer, respecting the send window (blocking until granted). */
    def write(data: Array[Byte]): Unit = {
      var off = 0
      while (off < data.length) {
        var grant = 0L
        windowGate.synchronized {
          while (sendWindow <= 0) windowGate.wait(5000)
          grant = math.min(sendWindow, math.min(data.length - off, 32 * 1024).toLong)
          sendWindow -= grant
        }
        val chunk = java.util.Arrays.copyOfRange(data, off, off + grant.toInt)
        sendFrame(T_DATA, 0, id, chunk.length, chunk)
        off += grant.toInt
      }
    }

    def fin(): Unit = sendFrame(T_WINDOW_UPDATE, F_FIN, id, 0, null)

    def grantWindow(delta: Long): Unit = windowGate.synchronized {
      sendWindow += delta
      windowGate.notifyAll()
    }
  }

  private val streams = new ConcurrentHashMap[Long, Stream]()

  /** Open a stream to `host:port`, sending SYN + OpenRequest. Returns the Stream. */
  def openStream(host: String, port: Int): Stream = {
    val id = nextId.getAndAdd(2)
    val s = new Stream(id)
    streams.put(id, s)
    // SYN with a 16 MiB window delta so our recv window is large (DOWN capacity).
    sendFrame(T_WINDOW_UPDATE, F_SYN, id, (CLIENT_WINDOW - INITIAL_WINDOW).toLong, null)
    // OpenRequest: 4-byte BE length + JSON.
    val json = s"""{"network":"tcp","address":"$host:$port"}""".getBytes(StandardCharsets.UTF_8)
    val payload = ByteString.newBuilder
      .putInt(json.length)(java.nio.ByteOrder.BIG_ENDIAN)
      .putBytes(json)
      .result()
      .toArray
    s.write(payload)
    s
  }

  // ---- frame IO ----

  private def sendFrame(t: Int, flags: Int, streamId: Long, lenOrDelta: Long, data: Array[Byte]): Unit = {
    val bb = ByteString.newBuilder
    bb.putByte(VERSION)
    bb.putByte(t.toByte)
    bb.putShort(flags)(java.nio.ByteOrder.BIG_ENDIAN)
    bb.putInt((streamId & 0xFFFFFFFFL).toInt)(java.nio.ByteOrder.BIG_ENDIAN)
    bb.putInt((lenOrDelta & 0xFFFFFFFFL).toInt)(java.nio.ByteOrder.BIG_ENDIAN)
    if (data != null) bb.putBytes(data)
    Await.result(outQueue.offer(BinaryMessage(bb.result())), 5.seconds)
  }

  private def onWsBytes(data: ByteString): Unit = {
    accum = accum ++ data
    parseLoop()
  }

  private def parseLoop(): Unit = {
    var continue = true
    while (continue) {
      if (accum.length < HEADER) { continue = false }
      else {
        val h = accum.take(HEADER)
        val bb = h.asByteBuffer
        bb.get() // version
        val t = bb.get() & 0xFF
        val flags = bb.getShort() & 0xFFFF
        val sid = bb.getInt().toLong & 0xFFFFFFFFL
        val len = bb.getInt().toLong & 0xFFFFFFFFL
        if (t == T_DATA) {
          if (accum.length < HEADER + len) { continue = false }
          else {
            val payload = accum.slice(HEADER, HEADER + len.toInt).toArray
            accum = accum.drop(HEADER + len.toInt)
            handleData(sid, flags, payload)
          }
        } else {
          accum = accum.drop(HEADER)
          t match {
            case T_WINDOW_UPDATE => handleWindowUpdate(sid, flags, len)
            case T_PING =>
              if ((flags & F_SYN) != 0) sendFrame(T_PING, F_ACK, 0, len, null)
            case T_GOAWAY => // ignore
            case _        => // ignore
          }
        }
      }
    }
  }

  private def handleData(sid: Long, flags: Int, payload: Array[Byte]): Unit = {
    val s = streams.get(sid)
    if (s != null) {
      if (payload.nonEmpty) s.inbound.put(Right(payload))
      if ((flags & F_FIN) != 0 || (flags & F_RST) != 0) s.inbound.put(Left(()))
    }
  }

  private def handleWindowUpdate(sid: Long, flags: Int, delta: Long): Unit = {
    val s = streams.get(sid)
    if (s != null) {
      if (delta > 0) s.grantWindow(delta)
      if ((flags & F_FIN) != 0 || (flags & F_RST) != 0) s.inbound.put(Left(()))
    }
  }

  def close(): Unit = {
    try outQueue.complete() catch { case _: Throwable => () }
  }
}
