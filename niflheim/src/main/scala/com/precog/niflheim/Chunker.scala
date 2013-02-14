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
package com.precog.niflheim

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{ ReadableByteChannel, WritableByteChannel }
import java.util.zip.Adler32

import scalaz._

/**
 * This class provides some nice method for writing/reading bytes to channels.
 * It does this by writing the data out in chunks. These chunks are fixed and
 * can use a checksum to ensure our data isn't corrupted.
 */
trait Chunker {
  def verify: Boolean
  val ChunkSize = 4096

  private def allocate(size: Int): ByteBuffer = ByteBuffer.allocate(size)

  private def takeChunk(buffer: ByteBuffer): ByteBuffer = {
    val bytes = new Array[Byte](ChunkSize)
    val remaining = buffer.remaining()
    val len = math.min(ChunkSize - 12, remaining)
    buffer.get(bytes, 4, len)

    val checksum = new Adler32()
    checksum.update(bytes, 4, ChunkSize - 12)

    val chunk = ByteBuffer.wrap(bytes)
    chunk.putInt(0, remaining)
    chunk.putLong(ChunkSize - 8, checksum.getValue())
    chunk
  }

  private def readChunk(chunk: ByteBuffer, buffer: ByteBuffer): Int = {
    chunk.mark()
    val remaining = chunk.getInt()
    val bytes = new Array[Byte](ChunkSize - 12)
    val len = math.min(remaining, bytes.length)
    chunk.get(bytes, 0, len)
    chunk.reset()

    if (verify) {
      val checksum = new Adler32()
      checksum.update(bytes)
      val sum0 = checksum.getValue()
      val sum1 = chunk.getLong(ChunkSize - 8)
      if (sum0 != sum1)
        throw new IOException("Corrupted chunk.")
    }

    buffer.put(bytes, 0, len)
    remaining - len
  }

  def write[A](channel: WritableByteChannel, maxSize: Int)(f: ByteBuffer => A): Validation[IOException, A] = {
    val buffer = allocate(maxSize)
    val result = f(buffer)
    buffer.flip()

    try {
      while (buffer.remaining() > 0) {
        val chunk = takeChunk(buffer)
        while (chunk.remaining() > 0) {
          channel.write(chunk)
        }
      }
      Success(result)
    } catch { case ex: IOException =>
      Failure(ex)
    }
  }

  def read(channel: ReadableByteChannel): Validation[IOException, ByteBuffer] = {
    try {
      val chunk = allocate(ChunkSize)
      while (chunk.remaining() > 0) {
        channel.read(chunk)
      }
      chunk.flip()

      val length = chunk.getInt(0)
      val buffer = allocate(length)
      while (readChunk(chunk, buffer) > 0) {
        chunk.clear()
        while (chunk.remaining() > 0) {
          channel.read(chunk)
        }
        chunk.flip()
      }
      buffer.flip()

      Success(buffer)
    } catch { case ioe: IOException =>
      Failure(ioe)
    }
  }
}
