package io.tunproto.scala.internal

import akka.util.ByteString
import io.netty.buffer.{ByteBuf, Unpooled}

/**
 * ByteBuf interop helpers centralizing all refcount discipline. Golden rule: copy
 * at every core<->Akka and core<->blocking boundary (we never hand a live ByteBuf
 * across threads), and release exactly once every buffer we own.
 */
private[internal] object ByteBufs {

  /**
   * Wrap an inbound byte array as a refCnt=1 heap ByteBuf. Used for
   * `session.receive` (which copies) and `stream.write` (which takes ownership).
   */
  def wrapInbound(bytes: Array[Byte]): ByteBuf = Unpooled.wrappedBuffer(bytes)

  /** Copy the readable bytes to a ByteString. Does NOT release; caller releases. */
  def toByteString(buf: ByteBuf): ByteString = {
    val a = new Array[Byte](buf.readableBytes())
    buf.getBytes(buf.readerIndex(), a)
    ByteString.fromArrayUnsafe(a)
  }

  /** Copy the readable bytes to a fresh array. Does NOT release; caller releases. */
  def toArray(buf: ByteBuf): Array[Byte] = {
    val a = new Array[Byte](buf.readableBytes())
    buf.getBytes(buf.readerIndex(), a)
    a
  }

  /** Copy the readable bytes to a fresh array, then release the buffer. */
  def toArrayAndRelease(buf: ByteBuf): Array[Byte] =
    try toArray(buf)
    finally releaseQuietly(buf)

  /** Release a buffer if it is non-null and still referenced. Idempotent-ish. */
  def releaseQuietly(buf: ByteBuf): Unit =
    if (buf != null && buf.refCnt() > 0) buf.release()
}
