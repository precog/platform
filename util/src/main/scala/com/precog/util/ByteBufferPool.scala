package com.precog
package util

import java.nio.ByteBuffer

import java.util.concurrent.{ BlockingQueue, ArrayBlockingQueue, LinkedBlockingQueue }

import java.lang.ref.SoftReference


final class ByteBufferPool(val capacity: Int = 16 * 1024, fixedBufferCount: Int = 8) {
  val fixedBufferQueue: BlockingQueue[ByteBuffer] = new ArrayBlockingQueue(fixedBufferCount)
  val flexBufferQueue: BlockingQueue[SoftReference[ByteBuffer]] = new LinkedBlockingQueue()

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
      buffer = ByteBuffer.allocate(capacity)
    }

    buffer.clear()
    buffer
  }

  def release(buffer: ByteBuffer): Unit = {
    if (!(fixedBufferQueue offer buffer)) {
      flexBufferQueue offer (new SoftReference(buffer))
    }
  }
}



