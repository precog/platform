package com.precog.yggdrasil.jdbm3

import scala.annotation.tailrec
import java.io._
import org.apache.jdbm.Serializer


object ByteArraySerializer extends Serializer[Array[Byte]] with Serializable {

  @tailrec
  private def writePackedInt(out: DataOutput, n: Int): Unit = if ((n & ~0x7F) != 0) {
    out.writeByte(n & 0x7F | 0x80)
    writePackedInt(out, n >> 7)
  } else {
    out.writeByte(n & 0x7F)
  }

  private def readPackedInt(in: DataInput): Int = {
    @tailrec def loop(n: Int, offset: Int): Int = {
      val b = in.readByte()
      if ((b & 0x80) != 0) {
        loop(n | ((b & 0x7F) << offset), offset + 7)
      } else {
        n | ((b & 0x7F) << offset)
      }
    }
    loop(0, 0)
  }

  def serialize(out: DataOutput, bytes: Array[Byte]) {
    writePackedInt(out, bytes.length)
    out.write(bytes)
  }

  def deserialize(in: DataInput): Array[Byte] = {
    val length = readPackedInt(in)
    val bytes = new Array[Byte](length)
    in.readFully(bytes)
    bytes
  }
}


