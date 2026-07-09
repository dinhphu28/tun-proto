package io.tunproto.scala.internal

import java.util.concurrent.{ExecutorService, Executors, ThreadFactory}
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService, Future, Promise}

/**
 * A single-thread, virtual-thread-backed [[ExecutionContext]] that serializes
 * EVERY interaction with one [[io.tunproto.yamux.YamuxSession]] and its streams.
 * This is the enforcement point of the yamux single-threaded contract: because a
 * single-thread executor runs at most one task at a time in submission order, as
 * long as all core calls (and the synchronous core callbacks they trigger) happen
 * via [[submit]] / [[call]] on this EC, the contract holds with zero locks.
 */
private[internal] final class SessionEC(name: String) {

  // Capture the one worker thread so callers can assert they are NOT on it before
  // blocking on a Future the session thread must complete (deadlock guard).
  @volatile private var worker: Thread = _

  private val factory: ThreadFactory = new ThreadFactory {
    private val delegate = Thread.ofVirtual().name(name).factory()
    override def newThread(r: Runnable): Thread = {
      val t = delegate.newThread(() => {
        worker = Thread.currentThread()
        r.run()
      })
      t
    }
  }

  private val exec: ExecutorService = Executors.newSingleThreadExecutor(factory)

  implicit val ec: ExecutionContextExecutorService =
    ExecutionContext.fromExecutorService(exec)

  @volatile private var stopped = false

  /** True if the current thread is the single session worker thread. */
  def isSessionThread: Boolean = Thread.currentThread() eq worker

  /**
   * Submit a side-effecting core action onto the session thread. Silently drops
   * the task after shutdown (the session is already gone).
   */
  def submit(body: => Unit): Unit =
    if (!stopped) {
      try exec.execute(() => body)
      catch { case _: java.util.concurrent.RejectedExecutionException => () }
    }

  /**
   * Submit and get a Future back. After shutdown the Future fails promptly so any
   * reader parked on it is released rather than hanging forever.
   */
  def call[A](body: => A): Future[A] =
    if (stopped) {
      Future.failed(new IllegalStateException("session EC stopped"))
    } else {
      val p = Promise[A]()
      try exec.execute(() => p.complete(scala.util.Try(body)))
      catch {
        case e: java.util.concurrent.RejectedExecutionException => p.failure(e)
      }
      p.future
    }

  def shutdown(): Unit = {
    stopped = true
    ec.shutdown()
  }
}
