package io.tunproto.scala.internal

import io.netty.buffer.ByteBuf
import io.tunproto.scala.TunnelOptions
import io.tunproto.yamux.{OpenRequestReader, ProtocolException, YamuxStream}

import java.net.{InetSocketAddress, Socket}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/**
 * Splices one accepted yamux stream to a freshly dialed target TCP socket, with
 * full bidirectional backpressure and correct WINDOW_UPDATE replenish accounting.
 *
 * ==Threading==
 *   - The four stream handlers (data/end/close/drain) and every core call
 *     (`consumed`, `write`, `writeQueueFull`, `close`, `reset`) run on the SESSION
 *     EC. That is where this class's `on*` methods execute.
 *   - Blocking target socket IO (connect/read/write) runs on the BLOCKING EC and
 *     never touches the core directly; results hop back via `sessionEC.submit`.
 *
 * ==Ordering invariants (the whole game)==
 *   - Forward the OpenRequest leftover to the target FIRST (no dropped bytes).
 *   - Call `stream.consumed(n)` only AFTER the target socket accepts n bytes.
 *   - UP writes to the socket are strictly serialized (a per-stream write chain
 *     built on the session EC) so bytes are never reordered.
 *   - DOWN backpressure: park the target reader while `writeQueueFull()`, released
 *     by `drainHandler` via a single non-recycled promise checked atomically with
 *     `writeQueueFull()` in the same session-EC task (avoids the lost-wakeup race).
 */
private[internal] final class StreamSplicer(
    options: TunnelOptions,
    stream: YamuxStream,
    sessionEC: SessionEC,
    blockingEc: ExecutionContext,
    activeStreamsCtr: AtomicInteger
) {

  private implicit val sec: ExecutionContext = sessionEC.ec

  private val reader = new OpenRequestReader()

  @volatile private var socket: Socket = _
  private var dialing = false
  private var spliced = false

  // One-shot terminal gate (session EC). First thing checked in cleanup().
  private val terminated = new AtomicBoolean(false)

  private var targetHost: String = _
  private var targetPort: Int = 0

  // OpenRequest header/leftover bytes to replenish once accepted by the target.
  private var openRequestConsumed = 0

  // UP-direction write chain: serializes socket writes in arrival order. Mutated
  // only on the session EC.
  private var writeChain: Future[Unit] = Future.unit

  // Bytes that arrive after the OpenRequest but before the socket connects. Held on
  // the session EC and flushed (in order) once connected. Their byte count is part
  // of the recvWindow debit and is replenished via forwardToTarget's consumed().
  private val pending = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]

  // DOWN-direction backpressure gate. Set on the session EC when the send queue is
  // full; completed on the session EC by drainHandler. The reader (blocking EC)
  // awaits the returned future. Never recycled/swapped => no lost wakeup.
  private var drainPromise: Promise[Unit] = _

  /** Wire the stream handlers synchronously. MUST be called on the session EC. */
  def begin(): Unit = {
    stream.dataHandler(onInboundData)
    stream.endHandler(() => onPeerFin())
    stream.closeHandler(() => onStreamFullyClosed())
    stream.drainHandler(() => onDrain())
  }

  // ---- Phase 1: read the OpenRequest (session EC), then dial (blocking EC) ----

  // dataHandler: called on the session EC; we OWN `buf`.
  private def onInboundData(buf: ByteBuf): Unit = {
    if (terminated.get()) {
      ByteBufs.releaseQuietly(buf)
      return
    }
    if (spliced) {
      // Phase 2 UP: copy off the mutating session accumulator, then forward.
      forwardToTarget(ByteBufs.toArrayAndRelease(buf))
      return
    }
    if (dialing) {
      // Bytes arriving while dialing: buffer until the socket connects, then flush
      // in order. We must NOT touch the socket yet (it is null until onConnected).
      pending += ByteBufs.toArrayAndRelease(buf)
      return
    }
    // Still parsing the OpenRequest. `offer` consumes+releases `buf` (even on throw
    // it releases the chunk; on a throw before completion `accum` leaks, so we call
    // reader.release() in cleanup()).
    val result =
      try reader.offer(buf)
      catch {
        case _: ProtocolException =>
          cleanupWithReset()
          return
      }
    if (result == null) return // need more bytes

    val req = result.request
    try {
      targetHost = req.host()
      targetPort = req.port()
    } catch {
      case _: ProtocolException =>
        ByteBufs.releaseQuietly(result.leftover)
        cleanupWithReset()
        return
    }

    // Egress policy (session EC, non-blocking).
    val allowed = options.allowTarget.forall(_(targetHost, targetPort))
    if (!allowed) {
      ByteBufs.releaseQuietly(result.leftover)
      cleanupWithReset()
      return
    }

    // These bytes already debited recvWindow; replenish once the leftover is
    // accepted by the target (or immediately if there is no leftover).
    openRequestConsumed = result.consumedBytes
    val leftover: Array[Byte] = ByteBufs.toArrayAndRelease(result.leftover)

    dialing = true
    val host = targetHost
    val port = targetPort
    val connectTimeoutMs = options.dialTimeout.toMillis.toInt

    Future {
      val s = new Socket()
      s.connect(new InetSocketAddress(host, port), connectTimeoutMs)
      s.setTcpNoDelay(true)
      s
    }(blockingEc).onComplete {
      case Success(s) => sessionEC.submit(onConnected(s, leftover))
      case Failure(_) => sessionEC.submit(cleanupWithReset())
    }(sessionEC.ec)
  }

  // ---- Phase 2: spliced ----

  // session EC
  private def onConnected(s: Socket, leftover: Array[Byte]): Unit = {
    if (terminated.get()) {
      // Torn down while dialing: close the freshly opened socket off-thread.
      closeSocketQuietly(s)
      return
    }
    socket = s
    dialing = false
    spliced = true

    // Forward the OpenRequest leftover FIRST (no dropped bytes), then any DATA that
    // arrived while dialing, in order. Replenish accounting is honored per chunk;
    // if there is nothing at all to forward, replenish the OpenRequest bytes now.
    val hasWork = leftover.nonEmpty || pending.nonEmpty
    if (leftover.nonEmpty) forwardToTarget(leftover)
    val queued = pending.toVector
    pending.clear()
    queued.foreach(forwardToTarget)
    if (!hasWork) replenishOpenRequest()

    startTargetReader()
  }

  /**
   * UP: forward one payload to the target, in order, then replenish. Appends to the
   * per-stream write chain on the session EC so socket writes never interleave.
   * `stream.consumed` fires after each chunk's write completes.
   */
  private def forwardToTarget(bytes: Array[Byte]): Unit = {
    val n = bytes.length
    writeChain = writeChain.transformWith { _ =>
      // Chain link runs on the session EC. Do the blocking write on the blocking EC.
      if (terminated.get()) {
        Future.unit
      } else {
        Future {
          val out = socket.getOutputStream
          if (n > 0) { out.write(bytes); out.flush() }
        }(blockingEc).transform { tryRes =>
          // Hop the completion effect back onto the session EC.
          sessionEC.submit {
            tryRes match {
              case Success(_) =>
                if (n > 0) stream.consumed(n)
                replenishOpenRequest()
              case Failure(_) =>
                cleanupWithReset()
            }
          }
          // Keep the chain alive regardless so later chunks still serialize.
          scala.util.Success(())
        }(blockingEc)
      }
    }
  }

  /** Replenish the OpenRequest header/leftover bytes exactly once (session EC). */
  private def replenishOpenRequest(): Unit =
    if (openRequestConsumed > 0) {
      val c = openRequestConsumed
      openRequestConsumed = 0
      stream.consumed(c)
    }

  /** DOWN: read the target socket on the blocking EC, write to the peer stream. */
  private def startTargetReader(): Unit =
    Future {
      val in = socket.getInputStream
      val buf = new Array[Byte](32 * 1024) // matches the client's 32 KiB copy buffer
      var eof = false
      while (!eof && !terminated.get()) {
        val read = in.read(buf)
        if (read < 0) eof = true
        else if (read > 0) {
          val chunk = java.util.Arrays.copyOf(buf, read)
          // Write to the stream on the session EC and read back writeQueueFull, plus
          // install the drain promise atomically if full. Blocking a virtual thread
          // is cheap; the session thread does the core call.
          val waitOn: Future[Unit] = awaitOnSession {
            if (terminated.get()) {
              Future.successful(()) // don't park; teardown in progress
            } else {
              stream.write(ByteBufs.wrapInbound(chunk)) // ownership -> stream
              if (stream.writeQueueFull()) {
                // Install a fresh promise ONLY here, on the session EC, in the same
                // task that observed writeQueueFull()==true. drainHandler completes
                // this exact reference (no swap) -> no lost wakeup.
                drainPromise = Promise[Unit]()
                drainPromise.future
              } else {
                Future.successful(())
              }
            }
          }
          // Park the (virtual) reader until the send queue drains below LOW_WATER.
          if (!waitOn.isCompleted) {
            try Await.result(waitOn, Duration.Inf)
            catch { case _: Throwable => eof = true }
          }
        }
      }
      // Target EOF: half-close the peer write side (FIN). Keep processing peer->
      // target until the peer's own FIN, exactly like the vertx reference.
      sessionEC.submit(if (!terminated.get()) stream.close())
    }(blockingEc)

  /**
   * Run `body` on the session EC and block the current (virtual, blocking-EC)
   * thread for the result. Must never be called from the session thread itself.
   */
  private def awaitOnSession[A](body: => A): A = {
    require(!sessionEC.isSessionThread, "awaitOnSession called from the session thread")
    Await.result(sessionEC.call(body), Duration.Inf)
  }

  // ---- handlers on the session EC ----

  // drainHandler: session EC. Complete the exact promise the reader is waiting on.
  private def onDrain(): Unit = {
    val p = drainPromise
    if (p != null) {
      drainPromise = null
      p.trySuccess(())
    }
  }

  // endHandler: peer FIN. Do NOT close the target; keep it open for its response.
  private def onPeerFin(): Unit = ()

  // closeHandler: both sides closed. Terminal cleanup.
  private def onStreamFullyClosed(): Unit = cleanup()

  // ---- teardown ----

  /** RST the stream (fires closeHandler -> cleanup). Session EC. */
  private def cleanupWithReset(): Unit = {
    if (!terminated.get()) stream.reset()
    // stream.reset() marks both sides closed and fires closeHandler synchronously
    // -> onStreamFullyClosed -> cleanup(). Guard in case closeHandler already fired.
    cleanup()
  }

  /** Idempotent terminal cleanup. Session EC. */
  private def cleanup(): Unit = {
    if (!terminated.compareAndSet(false, true)) return
    pending.clear()
    reader.release() // idempotent; frees accum on the error path
    // Release any parked reader.
    val p = drainPromise
    if (p != null) { drainPromise = null; p.trySuccess(()) }
    val s = socket
    if (s != null) closeSocketQuietly(s)
    activeStreamsCtr.decrementAndGet()
  }

  private def closeSocketQuietly(s: Socket): Unit =
    Future {
      try s.close()
      catch { case _: Throwable => () }
    }(blockingEc)
}
