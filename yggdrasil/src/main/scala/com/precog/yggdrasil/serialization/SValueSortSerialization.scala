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

trait BaseSortSerialization[A] extends SortSerialization[A] {
  final val HeaderFlag = 0
  final val ValueFlag = 1

  type Header

  def headerFor(value: A): Header

  def write(out: DataOutputStream, values: Array[A]): Unit = {
    @tailrec def write(i: Int, header: Option[Header]): Unit = {
      if (i < values.length) {
        val sv = values(i)
        val newHeader = headerFor(sv)
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

  def writeHeader(out: DataOutputStream, header: Header): Unit
  def writeRecord(out: DataOutputStream, value: A): Unit 

  def reader(in: DataInputStream): Iterator[A] = {
    new Iterator[A] {
      private val remaining: Int = in.readInt()
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
              assert (header != null)
              readRecord(in, header)
          }
        } else {
          null.asInstanceOf[A]
        }
      }
    }
  }

  def readHeader(in: DataInputStream): Header
  def readRecord(in: DataInputStream, header: Header): A
}

trait SValueSortSerialization extends BaseSortSerialization[SValue] with BinarySValueSerialization {
  case class Header(structure: Seq[(JPath, ColumnType)])

  def headerFor(value: SValue) = Header(value.structure)

  def writeHeader(out: DataOutputStream, header: Header): Unit = {
    out.writeInt(HeaderFlag)
    writeStructure(out, header.structure)
  }

  def writeRecord(out: DataOutputStream, sv: SValue): Unit = {
    out.writeInt(ValueFlag)
    writeValue(out, sv)
  }

  def readHeader(in: DataInputStream): Header = Header(readStructure(in))
  def readRecord(in: DataInputStream, header: Header) = readValue(in, header.structure)
}

trait SEventSortSerialization extends BaseSortSerialization[SEvent] with BinarySValueSerialization {
  case class Header(idCount: Int, structure: Seq[(JPath, ColumnType)])

  def headerFor(value: SEvent) = Header(value._1.length, value._2.structure)

  def writeHeader(out: DataOutputStream, header: Header): Unit = {
    out.writeInt(HeaderFlag)
    out.writeInt(header.idCount)
    writeStructure(out, header.structure)
  }

  def writeRecord(out: DataOutputStream, sv: SEvent): Unit = {
    out.writeInt(ValueFlag)
    writeIdentities(out, sv._1)
    writeValue(out, sv._2)
  }

  def readHeader(in: DataInputStream): Header = {
    Header(in.readInt(), readStructure(in))
  }

  def readRecord(in: DataInputStream, header: Header) = {
    (readIdentities(in, header.idCount), readValue(in, header.structure))
  }
}

trait GroupSortSerialization extends BaseSortSerialization[(SValue, Identities, SValue)] with BinarySValueSerialization {
  case class Header(keyStructure: Seq[(JPath, ColumnType)], idCount: Int, valueStructure: Seq[(JPath, ColumnType)])

  def headerFor(value: (SValue, Identities, SValue)) = Header(value._1.structure, value._2.length, value._3.structure)

  def writeHeader(out: DataOutputStream, header: Header): Unit = {
    out.writeInt(HeaderFlag)
    writeStructure(out, header.keyStructure)
    out.writeInt(header.idCount)
    writeStructure(out, header.valueStructure)
  }

  def writeRecord(out: DataOutputStream, sv: (SValue, Identities, SValue)): Unit = {
    out.writeInt(ValueFlag)
    writeValue(out, sv._1)
    writeIdentities(out, sv._2)
    writeValue(out, sv._3)
  }

  def readHeader(in: DataInputStream): Header = {
    Header(readStructure(in), in.readInt(), readStructure(in))
  }

  def readRecord(in: DataInputStream, header: Header) = {
    (readValue(in, header.keyStructure), readIdentities(in, header.idCount), readValue(in, header.valueStructure))
  }
}

// vim: set ts=4 sw=4 et:
