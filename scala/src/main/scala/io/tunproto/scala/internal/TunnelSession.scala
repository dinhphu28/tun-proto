package io.tunproto.scala.internal

import akka.actor.{ActorSystem, Cancellable}
import akka.stream.scaladsl.SourceQueueWithComplete
import akka.util.ByteString
import io.netty.buffer.ByteBuf
import io.tunproto.scala.TunnelOptions
import io.tunproto.yamux.{OutboundHandler, StreamHandler, YamuxConfig, YamuxConstants, YamuxSession, YamuxStream}

import java.time.{Duration => JDuration}
import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

/**
 * One tunnel session per WebSocket connection. Owns exactly one [[YamuxSession]],
 * the session EC (single virtual thread), the outbound queue bridge, and the
 * keepalive schedule. Every method here that touches the core runs on the session
 * EC.
 */
private[internal] final class TunnelSession(
    options: TunnelOptions,
    sessionEC: SessionEC,
    outQueue: SourceQueueWithComplete[ByteString],
    blockingEc: ExecutionContext,
    activeStreamsCtr: AtomicInteger,
    onTeardown: () => Unit
)(implicit system: ActorSystem) {

  private implicit val sec: ExecutionContext = sessionEC.ec

  // Assigned on the session EC in startSession().
  var session: YamuxSession = _

  private var keepAlive: Cancellable = Cancellable.alreadyCancelled
  private var tornDown = false

  // Outbound offer pipeline (all mutated on the session EC): offers are chained so
  // at most one is in flight and frames stay strictly FIFO (Akka's Source.queue
  // requires not overlapping offers). `pendingOffers` counts frames queued but not
  // yet enqueued into the stream; pauseOutbound when it goes positive,
  // resumeOutbound when it returns to zero (transport backpressure).
  private var offerTail: Future[Unit] = Future.unit
  private var pendingOffers = 0
  private var outboundPaused = false
  // Watermarks for outbound transport backpressure (queue bufferSize is 256).
  private val OutboundPauseHigh = 200
  private val OutboundPauseLow = 50

  /** Build the YamuxSession and register keepalive. MUST run on the session EC. */
  def startSession(): Unit = {
    val config = YamuxConfig
      .builder()
      .maxStreamWindow(options.maxStreamWindow)
      .maxConcurrentStreams(options.maxConcurrentStreamsPerSession)
      .keepAliveInterval(JDuration.ofMillis(options.keepAliveInterval.toMillis))
      .keepAliveTimeout(JDuration.ofMillis(options.keepAliveTimeout.toMillis))
      .build()

    val outHandler: OutboundHandler = (frame: ByteBuf) => enqueueOutbound(frame)
    val streamHandler: StreamHandler = (stream: YamuxStream) => onNewStream(stream)

    session = new YamuxSession(config, outHandler, streamHandler)

    // Keepalive: ping + timeout enforcement, always hopped to the session EC.
    val interval = options.keepAliveInterval
    keepAlive = system.scheduler.scheduleWithFixedDelay(interval, interval) { () =>
      sessionEC.submit {
        if (!tornDown && !session.isClosed) {
          session.sendPing()
          session.tick(System.currentTimeMillis())
          if (session.isClosed) teardown() // pong timeout tripped -> close the WS
        }
      }
    }(system.dispatcher)
  }

  // ---- inbound (session EC): feed WS bytes to the core ----

  def receive(bytes: Array[Byte]): Unit = {
    if (tornDown || session == null || session.isClosed) return
    val buf = ByteBufs.wrapInbound(bytes)
    try session.receive(buf)
    finally ByteBufs.releaseQuietly(buf)
  }

  // ---- outbound (session EC): OutboundHandler.onFrame ----

  /**
   * We OWN `frame`. Copy to a ByteString (safe to release synchronously since the
   * ByteString owns its own array), then enqueue behind any in-flight offer so
   * frames are emitted strictly FIFO with single-in-flight backpressure.
   */
  private def enqueueOutbound(frame: ByteBuf): Unit = {
    val bs = ByteBufs.toByteString(frame)
    ByteBufs.releaseQuietly(frame)

    // A frame is now queued but not yet accepted by the WS stream. Engage transport
    // backpressure with hysteresis: pause the core only when the chain gets deep
    // (real WS slowness), resume when it drains. Pausing on every frame would
    // ping-pong and can stall DATA flush, so we use HIGH/LOW watermarks.
    pendingOffers += 1
    if (!outboundPaused && pendingOffers >= OutboundPauseHigh
      && session != null && !session.isClosed) {
      outboundPaused = true
      session.pauseOutbound()
    }

    // Chain behind the previous offer so at most one offer is in flight (Akka's
    // Source.queue requires not overlapping offers) -> frames stay strictly FIFO.
    offerTail = offerTail.transformWith { _ =>
      if (tornDown) Future.unit
      else outQueue.offer(bs).transform { res =>
        // Hop the completion back to the session EC: decrement, resume when drained.
        sessionEC.submit {
          import akka.stream.QueueOfferResult
          res match {
            case Success(QueueOfferResult.Enqueued) => ()
            case _ =>
              // Dropped / QueueClosed / Failure -> the transport is gone.
              if (session != null && !session.isClosed) session.close()
              teardown()
          }
          pendingOffers -= 1
          if (outboundPaused && pendingOffers <= OutboundPauseLow
            && !tornDown && session != null && !session.isClosed) {
            outboundPaused = false
            session.resumeOutbound()
          }
        }
        scala.util.Success(())
      }(sessionEC.ec)
    }
  }

  // ---- new stream (session EC) ----

  private def onNewStream(stream: YamuxStream): Unit = {
    if (options.maxStreams > 0 && activeStreamsCtr.get() >= options.maxStreams) {
      stream.reset()
      return
    }
    activeStreamsCtr.incrementAndGet()
    new StreamSplicer(options, stream, sessionEC, blockingEc, activeStreamsCtr).begin()
  }

  // ---- teardown (session EC) ----

  /** Idempotent session teardown. */
  def teardown(): Unit = {
    if (tornDown) return
    tornDown = true
    keepAlive.cancel()
    if (session != null && !session.isClosed) {
      session.goAway(YamuxConstants.GOAWAY_NORMAL)
      session.close() // force-closes all streams -> per-stream closeHandler -> cleanup
    }
    // Complete the outbound queue so the WS source finishes and the coupled flow
    // closes the WebSocket.
    outQueue.complete()
    onTeardown()
  }

  /** Graceful shutdown from server.close(): GO_AWAY then teardown. */
  def shutdown(): Unit = sessionEC.submit(teardown())
}
