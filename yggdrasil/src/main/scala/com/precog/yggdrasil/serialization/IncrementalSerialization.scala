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
package com.precog.yggdrasil.serialization

import java.io._
import scala.annotation.tailrec

trait IncrementalSerialization[A] extends StreamSerialization with RunlengthFormatting[A] {
  final val EOFFlag = Int.MinValue + 2

  final class IncrementalWriter(header: Header) {
    def write(out: DataOutputStream, value: A): IncrementalWriter = {
      val newHeader = headerFor(value)
      if (header == newHeader) {
        out.writeInt(ValueFlag)
        writeRecord(out, value)
        this
      } else {
        out.writeInt(HeaderFlag)
        writeHeader(out, newHeader)
        out.writeInt(ValueFlag)
        writeRecord(out, value)
        new IncrementalWriter(newHeader)
      }
    }

    def finish(out: DataOutputStream): Unit = {
      out.writeInt(EOFFlag)
    }
  }

  final class IncrementalReader {
    def read(in: DataInputStream): Iterator[A] = {
      new Iterator[A] {
        private var header: Header = null.asInstanceOf[Header]
        private var _next = precomputeNext()

        def hasNext = _next != null
        def next: A = {
          if (_next == null) throw new IllegalStateException("next called on iterator beyond end of input")
          val tmp = _next
          _next = precomputeNext()
          tmp
        }

        @tailrec private def precomputeNext(): A = {
          in.readInt() match {
            case HeaderFlag =>
              header = readHeader(in)
              precomputeNext()

            case ValueFlag => 
              assert (header != null)
              readRecord(in, header)

            case EOFFlag =>
              null.asInstanceOf[A]
          }
        }
      }
    }
  }

  def writer: IncrementalWriter = new IncrementalWriter(null.asInstanceOf[Header])
  def reader: IncrementalReader = new IncrementalReader
}


// vim: set ts=4 sw=4 et:
