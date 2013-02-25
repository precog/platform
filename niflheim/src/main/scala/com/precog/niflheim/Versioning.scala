package com.precog.niflheim

import com.precog.util.PrecogUnit

import java.io.{ File, IOException }
import java.nio.channels.{ ReadableByteChannel, WritableByteChannel }
import java.nio.ByteBuffer

import scalaz.{ Validation, Success, Failure }

trait Versioning {
  def magic: Short
  def version: Short

  def writeVersion(channel: WritableByteChannel): Validation[IOException, PrecogUnit] = {
    val buffer = ByteBuffer.allocate(4)
    buffer.putShort(magic)
    buffer.putShort(version)
    buffer.flip()

    try {
      while (buffer.remaining() > 0) {
        channel.write(buffer)
      }
      Success(PrecogUnit)
    } catch { case ioe: IOException =>
      Failure(ioe)
    }
  }

  def readVersion(channel: ReadableByteChannel): Validation[IOException, Int] = {
    val buffer = ByteBuffer.allocate(4)
    try {
      while (buffer.remaining() > 0) {
        channel.read(buffer)
      }
      buffer.flip()

      val magic0: Short = buffer.getShort()
      val version0: Int = buffer.getShort()
      if (magic0 == magic) {
        Success(version0)
      } else {
        Failure(new IOException("Incorrect magic number found."))
      }
    } catch { case ioe: IOException =>
      Failure(ioe)
    }
  }
}


