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

import leveldb._
import com.precog.common.Path

import com.precog.common.VectorCase

import com.precog.util.Bijection._

import com.precog.yggdrasil._
import com.precog.yggdrasil.SValue._
import com.precog.yggdrasil.ColumnType._

import blueeyes.json._
import blueeyes.json.JPath._

import scala.actors.remote._

import java.io._
import scala.annotation.tailrec
import scalaz.effect._
import scalaz.syntax.monad._

trait BinarySValueSerialization {
  def writeStructure(out: DataOutputStream, structure: Seq[(JPath, ColumnType)]): Unit = {
    out.writeInt(structure.size)
    structure foreach {
      case (sel, valType) => {       
        out.writeUTF(sel.toString)
        out.writeUTF(nameOf(valType))
      }
    }
  }

  def readStructure(in: DataInputStream): Seq[(JPath, ColumnType)] = {
    @tailrec def loop(in: DataInputStream, acc: Seq[(JPath, ColumnType)], i: Int): Seq[(JPath, ColumnType)] = {
      if (i > 0) {
        val selector = JPath(in.readUTF())
        ColumnType.fromName(in.readUTF()) match {
          case Some(ctype) => loop(in, acc :+ ((selector, ctype)), i - 1)
          case None        => sys.error("Memoization header corrupt: unable to read back column type indicator.")
        }
      } else {
        acc
      }
    }

    val columns = in.readInt()
    loop(in, Vector.empty[(JPath, ColumnType)], columns)
  }

  def writeValue(out: DataOutputStream, sv: SValue): Unit = {
    def write(out: DataOutputStream, sv: SValue): Unit = {
      sv.fold(
        obj = obj       => obj.map { 
          case (_, v)   => write(out, v)
        },
        arr = arr       => arr.map(v => write(out, v)),
        str = str       => out.writeUTF(str),
        bool = bool     => out.writeBoolean(bool),
        long = long     => out.writeLong(long),
        double = double => out.writeDouble(double),
        num = num       => {
          val bytes = num.as[Array[Byte]]
          out.writeInt(bytes.length)
          out.write(bytes, 0, bytes.length)
        },
        nul = out.writeInt(0))
    }

    write(out, sv)
  }

  def readValue(in: DataInputStream, cols: Seq[(JPath, ColumnType)]): SValue = {
    cols.foldLeft(Option.empty[SValue]) {
      case (None     , (JPath.Identity, ctype)) => Some(readColumn(in, ctype).toSValue)
      case (None     , (jpath, ctype))          => 
        jpath.nodes match {
          case JPathIndex(_) :: xs => SArray(Vector()).set(jpath, readColumn(in, ctype))
          case JPathField(_) :: xs => SObject(Map()).set(jpath, readColumn(in, ctype))
        }
      case (Some(obj), (JPath.Identity, ctype)) => sys.error("Illegal data header: multiple values at a selector root.")
      case (Some(obj), (jpath, ctype))          => obj.set(jpath, readColumn(in, ctype))
    } getOrElse {
      SNull
    }
  }

  private def readColumn(in: DataInputStream, ctype: ColumnType): CValue = ctype match {
    case SStringFixed(_)   => CString(in.readUTF())
    case SStringArbitrary  => CString(in.readUTF())
    case SBoolean          => CBoolean(in.readBoolean())
    case SInt              => CInt(in.readInt())
    case SLong             => CLong(in.readLong())
    case SFloat            => CFloat(in.readFloat())
    case SDouble           => CDouble(in.readDouble())
    case SDecimalArbitrary => 
      val length = in.readInt()
      val sdecimalarb: Array[Byte] = new Array(length)
      in.read(sdecimalarb)
      CNum(sdecimalarb.as[BigDecimal])
    case SNull                  => {
      in.readInt()
      CNull
    }
    case SEmptyObject           => CEmptyObject
    case SEmptyArray            => CEmptyArray
  }
}

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

trait BinaryProjectionSerialization extends IterateeFileSerialization[Vector[SEvent]] with BinarySValueSerialization {
  case class Header(idCount: Int, structure: Seq[(JPath, ColumnType)])
  final val HeaderFlag = 0
  final val EventFlag = 1

  def writeElement(out: DataOutputStream, values: Vector[SEvent]): IO[Unit] = {
    val (_, result) = values.foldLeft((Option.empty[Header], IO(out.writeInt(values.size)))) {
      case ((oldHeader, io), (ids, sv)) => {
        val newHeader = Header(ids.size, sv.structure)
        if (oldHeader.exists(_ == newHeader)) {
          (oldHeader, io >> writeEvent(out, (ids, sv)))
        } else {
          (Some(newHeader), io >> writeHeader(out, newHeader) >> writeEvent(out, (ids, sv)))
        }
      }
    }
    
    result
  }
    
  def readElement(in: DataInputStream): IO[Option[Vector[SEvent]]] = {
    def loop(in: DataInputStream, acc: Vector[SEvent], i: Int, header: Option[Header]): IO[Option[Vector[SEvent]]] = { 
      if (i > 0) {
        IO(in.readInt()) flatMap {
          case HeaderFlag =>
            for {
              newHeader <- readHeader(in)
              result    <- loop(in, acc, i, Some(newHeader))
            } yield result

          case EventFlag => header match {
            case None    => IO(None)
            case Some(h) => 
              for {
                nextElement <- readEvent(in, h.idCount, h.structure)
                result      <- loop(in, acc :+ nextElement, i - 1, header)
              } yield result
          }
        }
      } else {
        if (acc.isEmpty) IO(None)
        else IO(Some(acc))
      }
    }

    {
      for {
        length <- IO(in.readInt())
        result <- loop(in, Vector.empty[SEvent], length, None)
      } yield result
    } except {
      case ex: java.io.EOFException => IO(None)
      case ex => ex.printStackTrace; IO(None)
    }
  }

  def writeHeader(out: DataOutputStream, header: Header): IO[Unit] = 
    IO { out.writeInt(HeaderFlag) ; out.writeInt(header.idCount) ; writeStructure(out, header.structure) }

  def readHeader(in: DataInputStream): IO[Header] = 
    IO { Header(in.readInt(), readStructure(in)) }

  def writeEvent(out: DataOutputStream, ev: SEvent): IO[Unit] = {
    for {
      _ <- IO { out.writeInt(EventFlag) }
      _ <- writeIdentities(out, ev._1)
      _ <- IO { writeValue(out, ev._2) }
    } yield ()
  }

  def writeIdentities(out: DataOutputStream, id: Identities): IO[Unit] = IO {
    id.map(out.writeLong(_))
  }

  def readEvent(in: DataInputStream, length: Int, cols: Seq[(JPath, ColumnType)]): IO[SEvent] = {
    for {
      ids <- readIdentities(in, length)
      sv  <- IO { readValue(in, cols) } 
    } yield (ids, sv)
  }

  def readIdentities(in: DataInputStream, length: Int): IO[Identities] = {
    @tailrec def loop(in: DataInputStream, acc: VectorCase[Long], i: Int): Identities = {
      if (i > 0) {
        loop(in, acc :+ in.readLong(), i - 1)
      } else {
        acc
      }
    }

    IO {
      loop(in, VectorCase.empty[Long], length)
    }
  }
}

