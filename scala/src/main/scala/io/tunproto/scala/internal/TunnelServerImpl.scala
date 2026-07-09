package io.tunproto.scala.internal

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.{Authorization, OAuth2BearerToken}
import akka.http.scaladsl.model.ws.{BinaryMessage, Message, TextMessage}
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{Materializer, OverflowStrategy}
import akka.util.ByteString
import io.tunproto.scala.{TunnelOptions, TunnelServer}

import java.security.MessageDigest
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Success

/**
 * Implementation of [[TunnelServer]]: option validation, the shared blocking EC,
 * the Akka HTTP route (Bearer auth -> WS upgrade), per-session engine wiring, the
 * session/stream gauges, and lifecycle.
 */
private[scala] final class TunnelServerImpl(options: TunnelOptions)(implicit system: ActorSystem)
    extends TunnelServer {

  private val log = system.log
  private implicit val mat: Materializer = Materializer(system)
  // EC for the stream-materialization glue (toStrict combinators). NOT a session EC.
  private implicit val streamEc: ExecutionContext = system.dispatcher

  // Blocking EC for target socket IO. Default = virtual-thread-per-task.
  private val ownsBlockingEc: Boolean = options.blockingEc.isEmpty
  private val blockingExecutor =
    if (ownsBlockingEc) Some(Executors.newVirtualThreadPerTaskExecutor()) else None
  private val blockingEc: ExecutionContext =
    options.blockingEc.getOrElse(ExecutionContext.fromExecutorService(blockingExecutor.get))

  private val activeSessionsCtr = new AtomicInteger(0)
  private val activeStreamsCtr = new AtomicInteger(0)
  private val sessionSeq = new AtomicLong(0)

  // Live sessions, so close() can GO_AWAY them. Guarded by its own monitor.
  private val liveSessions = mutable.Set.empty[TunnelSession]

  if (options.allowTarget.isEmpty) {
    log.warning(
      "tun-proto: no allowTarget policy set - the tunnel will dial ANY host:port a client " +
        "requests (SSRF risk). Set TunnelOptions.allowTarget to restrict egress."
    )
  }

  // ---- route ----

  def route: Route =
    path(separateOnSlashes(options.path)) {
      authenticateBearer { _ =>
        if (options.maxSessions > 0 && activeSessionsCtr.get() >= options.maxSessions) {
          complete(StatusCodes.ServiceUnavailable)
        } else {
          handleWebSocketMessages(buildFlow())
        }
      }
    }

  /** Bearer extraction + validation. Provides the token, else rejects with 401. */
  private def authenticateBearer(inner: String => Route): Route =
    optionalHeaderValueByType(Authorization) {
      case Some(Authorization(OAuth2BearerToken(token))) =>
        onComplete(validate(token)) {
          case Success(true) => inner(token)
          case _             => complete(StatusCodes.Unauthorized)
        }
      case _ =>
        if (options.authDisabled) inner("")
        else complete(StatusCodes.Unauthorized)
    }

  private def validate(token: String): Future[Boolean] =
    if (options.authDisabled) Future.successful(true)
    else
      options.authenticator match {
        case Some(fn) => fn(token)
        case None     => Future.successful(constantTimeContains(options.apiKeys, token))
      }

  private def constantTimeContains(keys: Set[String], token: String): Boolean = {
    val provided = token.getBytes(StandardCharsets.UTF_8)
    var matched = false
    keys.foreach { k =>
      if (MessageDigest.isEqual(provided, k.getBytes(StandardCharsets.UTF_8))) matched = true
    }
    matched
  }

  // ---- WS flow per connection ----

  private def buildFlow(): Flow[Message, Message, Any] = {
    val id = sessionSeq.incrementAndGet()
    val sessionEC = new SessionEC(s"yamux-session-$id")

    // OUTBOUND: bounded queue of frame ByteStrings, backpressure-aware.
    val (queue, outSource) =
      Source
        .queue[ByteString](bufferSize = 256, OverflowStrategy.backpressure)
        .preMaterialize()

    val outbound: Source[Message, Any] = outSource.map(bs => BinaryMessage(bs))

    // Forward reference so onTeardown (called from within TunnelSession.teardown)
    // can decrement the gauge exactly once, whether teardown was triggered by WS
    // termination or internally (pong timeout / outbound failure).
    var sessionRef: TunnelSession = null
    val removed = new java.util.concurrent.atomic.AtomicBoolean(false)
    def removeSession(): Unit =
      if (removed.compareAndSet(false, true)) {
        liveSessions.synchronized(liveSessions -= sessionRef)
        activeSessionsCtr.decrementAndGet()
      }

    val tunnelSession =
      new TunnelSession(
        options,
        sessionEC,
        queue,
        blockingEc,
        activeStreamsCtr,
        onTeardown = () => removeSession()
      )
    sessionRef = tunnelSession

    liveSessions.synchronized(liveSessions += tunnelSession)
    activeSessionsCtr.incrementAndGet()

    sessionEC.submit(tunnelSession.startSession())

    // INBOUND: fold streamed messages to strict, feed session.receive in order.
    val inbound: Sink[Message, Any] =
      Flow[Message]
        .flatMapConcat {
          case bm: BinaryMessage =>
            // Fold a (possibly streamed) binary message to strict, then emit its
            // bytes. Message boundaries are NOT yamux frame boundaries, so a strict
            // per-message ByteString is just a chunk of the byte stream. On failure
            // the Future fails -> the stream fails -> the session tears down (we make
            // it FATAL, never lossy).
            Source.future(bm.toStrict(5.seconds).map[ByteString](_.data))
          case tm: TextMessage =>
            // SPEC: ignore text frames. Drain then emit nothing.
            Source.future(tm.toStrict(1.second)).flatMapConcat(_ => Source.empty[ByteString])
        }
        .filter(_.nonEmpty)
        .to(Sink.foreach { (bytes: ByteString) =>
          sessionEC.submit(tunnelSession.receive(bytes.toArray))
        })

    Flow
      .fromSinkAndSourceCoupledMat(inbound, outbound)(Keep.right)
      .watchTermination() { (_, done) =>
        done.onComplete { _ =>
          sessionEC.submit {
            tunnelSession.teardown() // calls onTeardown -> removeSession()
            sessionEC.shutdown()
          }
        }(sessionEC.ec)
      }
  }

  // ---- listen ----

  def listen(host: String, port: Int): Future[Http.ServerBinding] =
    Http().newServerAt(host, port).bind(route)

  def activeSessions: Int = activeSessionsCtr.get()
  def activeStreams: Int = activeStreamsCtr.get()

  def close(): Future[Unit] = {
    val sessions = liveSessions.synchronized(liveSessions.toVector)
    sessions.foreach(_.shutdown())
    blockingExecutor.foreach(_.shutdown())
    Future.unit
  }
}
