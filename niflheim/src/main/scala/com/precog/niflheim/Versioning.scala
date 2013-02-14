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


