package com.precog
package util

import java.nio.ByteBuffer

import java.util.concurrent.{ BlockingQueue, ArrayBlockingQueue, LinkedBlockingQueue }
import java.util.concurrent.atomic.AtomicLong

import java.lang.ref.SoftReference

import scalaz._
import scalaz.State


/**
 * A `Monad` for working with `ByteBuffer`s.
 */
trait ByteBufferMonad[M[+_]] extends Monad[M] {
  def getBuffer(min: Int): M[ByteBuffer]
}


final class ByteBufferPool(val capacity: Int = 16 * 1024, fixedBufferCount: Int = 8, direct: Boolean = false) {
  import ByteBufferPool._

  private val _hits = new AtomicLong()
  private val _misses = new AtomicLong()

  val fixedBufferQueue: BlockingQueue[ByteBuffer] = new ArrayBlockingQueue(fixedBufferCount)
  val flexBufferQueue: BlockingQueue[SoftReference[ByteBuffer]] = new LinkedBlockingQueue()

  def hits = _hits.get()
  def misses = _hits.get()

  /**
   * Returns a cleared `ByteBuffer` that can store `capacity` bytes.
   */
  def acquire: ByteBuffer = {
    var buffer = fixedBufferQueue.poll()

    if (buffer == null) {
      var ref = flexBufferQueue.poll()
      buffer = if (ref != null) ref.get else null
      while (ref != null && buffer == null) {
        ref = flexBufferQueue.poll()
        buffer = if (ref != null) ref.get else null
      }
    }

    if (buffer == null) {
      _misses.incrementAndGet()
      buffer = if (direct) ByteBuffer.allocateDirect(capacity) else ByteBuffer.allocate(capacity)
    } else {
      _hits.incrementAndGet()
    }

    buffer.clear()
    buffer
  }

  /**
   * Releases a `ByteBuffer` back into the pool for re-use later on. This isn't
   * strictly required, but if `direct` is `true`, then you most certainly
   * should.
   */
  def release(buffer: ByteBuffer): Unit = {
    if (!(fixedBufferQueue offer buffer)) {
      flexBufferQueue offer (new SoftReference(buffer))
    }
  }

  def toStream: Stream[ByteBuffer] = Stream.continually(acquire)

  def run[A](a: ByteBufferPoolS[A]): A = a.eval((this, Nil))
}


object ByteBufferPool {

  type ByteBufferPoolS[+A] = State[(ByteBufferPool, List[ByteBuffer]), A]

  implicit object ByteBufferPoolMonad extends ByteBufferMonad[ByteBufferPoolS]
      with Monad[ByteBufferPoolS] {

    def point[A](a: => A): ByteBufferPoolS[A] = State.state(a)

    def bind[A, B](fa: ByteBufferPoolS[A])(f: A => ByteBufferPoolS[B]): ByteBufferPoolS[B] =
      State(s => fa(s) match {
        case (s, a) => f(a)(s)
      })

    def getBuffer(min: Int): ByteBufferPoolS[ByteBuffer] = ByteBufferPool.acquire(min)
  }

  /**
   * Acquire a `ByteBuffer` and add it to the state.
   */
  def acquire(min: Int): ByteBufferPoolS[ByteBuffer] = State {
    case (pool, buffers @ (buf :: _)) if buf.remaining() >= min =>
      ((pool, buffers), buf)

    case (pool, buffers) =>
      val buf = pool.acquire
      ((pool, buf :: buffers), buf)
  }

  def acquire: ByteBufferPoolS[ByteBuffer] = acquire(512)

  /**
   * Reverses the state (list of `ByteBuffer`s) and returns an `Array[Byte]` of
   * the contiguous bytes in all the buffers.
   */
  def flipBytes: ByteBufferPoolS[Array[Byte]] = State { case (pool, sreffub) =>
    val buffers = sreffub.reverse
    ((pool, buffers), getBytesFrom(buffers))
  }

  /**
   * Removes and releases all `ByteBuffer`s in the state to the pool.
   */
  def release: ByteBufferPoolS[Unit] = State { case (pool, buffers) =>
    buffers foreach (pool.release(_))
    ((pool, Nil), ())
  }

  def getBytesFrom(buffers: List[ByteBuffer]): Array[Byte] = {
    val size = buffers.foldLeft(0) { (size, buf) =>
      buf.flip()
      size + buf.remaining()
    }
    val bytes = Array.ofDim[Byte](size)
    buffers.foldLeft(0) { (offset, buf) =>
      val length = buf.remaining()
      buf.get(bytes, offset, length)
      offset + length
    }
    bytes
  }
}


