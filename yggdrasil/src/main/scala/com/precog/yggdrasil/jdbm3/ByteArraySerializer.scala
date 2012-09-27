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


