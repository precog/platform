package com.precog.niflheim

import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.{ ReadableByteChannel, WritableByteChannel }
import java.util.zip.Adler32

import scalaz._

trait Chunker {
  def verify: Boolean
  val ChunkSize = 4096

  private def allocate(size: Int): ByteBuffer = ByteBuffer.allocate(size)

  def takeChunk(buffer: ByteBuffer): ByteBuffer = {
    val bytes = new Array[Byte](ChunkSize)
    val remaining = buffer.remaining()
    val len = math.min(ChunkSize - 12, remaining)
    buffer.get(bytes, 0, len)

    val checksum = new Adler32()
    checksum.update(bytes, 4, ChunkSize - 8)

    val chunk = ByteBuffer.wrap(bytes)
    chunk.putInt(0, remaining)
    chunk.putLong(ChunkSize - 8, checksum.getValue())
    chunk
  }

  def readChunk(chunk: ByteBuffer, buffer: ByteBuffer): Int = {
    val remaining = chunk.getInt(0)

    val bytes = new Array[Byte](ChunkSize)
    val len = math.min(remaining, buffer.remaining())
    chunk.get(bytes, 0, ChunkSize)

    if (verify) {
      val checksum = new Adler32()
      checksum.update(bytes, 4, ChunkSize - 8)
      val sum0 = checksum.getValue()
      val sum1 = chunk.getLong(ChunkSize - 8)
      throw new IOException("Corrupted chunk.")
    }

    buffer.put(bytes, 4, len)
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

      val length = chunk.getInt(0)
      val buffer = allocate(length)
      while (readChunk(chunk, buffer) > 0) {
        chunk.clear()
        while (chunk.remaining() > 0) {
          channel.read(chunk)
        }
      }

      buffer.flip()
      Success(buffer)
    } catch { case ioe: IOException =>
        Failure(ioe)
    }
  }
}
