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
package yggdrasil
package serialization

import com.precog.common.VectorCase

import com.precog.yggdrasil.SValue._
import com.precog.yggdrasil.ColumnType._

import blueeyes.json._
import blueeyes.json.JPath._

import java.io._
import scala.annotation.tailrec
import scalaz.effect._
import scalaz.syntax.monad._

trait SValueSortSerialization extends SortSerialization[SValue] with BinarySValueSerialization {
  case class Header(structure: Seq[(JPath, ColumnType)])
  final val HeaderFlag = 0
  final val ValueFlag = 1

  def write(out: DataOutputStream, values: Array[SValue]): Unit = {
    @tailrec def write(i: Int, header: Option[Header]): Unit = {
      if (i < values.length) {
        val sv = values(i)
        val newHeader = Header(sv.structure)
        if (header.exists(_ == newHeader)) {
          writeRecord(out, sv)
          write(i + 1, header)
        } else {
          writeHeader(out, newHeader)
          writeRecord(out, sv)
          write(i + 1, Some(newHeader))
        }
      }
    }
    
    out.writeInt(values.length)
    write(0, None)
  }

  def writeHeader(out: DataOutputStream, header: Header): Unit = {
    out.writeInt(HeaderFlag)
    writeStructure(out, header.structure)
  }

  def writeRecord(out: DataOutputStream, sv: SValue): Unit = {
    out.writeInt(ValueFlag)
    writeValue(out, sv)
  }

  def reader(in: DataInputStream): Iterator[SValue] = {
    new Iterator[SValue] {
      private val remaining: Int = in.readInt()
      private var header: Header = null.asInstanceOf[Header]

      private var _next = precomputeNext()

      def hasNext = _next != null
      def next: SValue = {
        assert (_next != null)
        val tmp = _next
        _next = precomputeNext()
        tmp
      }

      @tailrec private def precomputeNext(): SValue = {
        if (remaining > 0) {
          in.readInt() match {
            case HeaderFlag =>
              header = readHeader(in)
              precomputeNext()

            case ValueFlag => 
              assert (header != null)
              readValue(in, header.structure)
          }
        } else {
          null.asInstanceOf[SValue]
        }
      }
    }
  }

  def readHeader(in: DataInputStream): Header = Header(readStructure(in))
}
// vim: set ts=4 sw=4 et:
