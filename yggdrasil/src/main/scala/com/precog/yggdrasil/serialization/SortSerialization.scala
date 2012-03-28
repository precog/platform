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
package com.precog.yggdrasil
package serialization

import java.io._
import scala.annotation.tailrec

trait SortSerialization[A] extends StreamSerialization with RunlengthFormatting[A] {
  // Write out from the buffer, indices [0,limit)
  def write(out: DataOutputStream, values: Array[A], limit: Int): Unit = {
    @tailrec def write(i: Int, header: Header): Unit = {
      if (i < limit) {
        val sv = values(i)
        val newHeader = headerFor(sv)
        if (header == newHeader) {
          out.writeInt(ValueFlag)
          writeRecord(out, sv, header)
          write(i + 1, header)
        } else {
          out.writeInt(HeaderFlag)
          writeHeader(out, newHeader)
          out.writeInt(ValueFlag)
          writeRecord(out, sv, newHeader)
          write(i + 1, newHeader)
        }
      } 
    }
    
    out.writeInt(limit)
    write(0, null.asInstanceOf[Header])
  }

  def reader(in: DataInputStream): Iterator[A] = {
    new Iterator[A] {
      private var remaining: Int = in.readInt()
      private var header: Header = null.asInstanceOf[Header]

      private var _next = precomputeNext()

      def hasNext = _next != null
      def next: A = {
        assert (_next != null)
        val tmp = _next
        _next = precomputeNext()
        tmp
      }

      @tailrec private def precomputeNext(): A = {
        if (remaining > 0) {
          in.readInt() match {
            case HeaderFlag =>
              header = readHeader(in)
              precomputeNext()

            case ValueFlag => 
              remaining -= 1
              assert (header != null)
              readRecord(in, header)
          }
        } else {
          null.asInstanceOf[A]
        }
      }
    }
  }
}
