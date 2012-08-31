/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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



