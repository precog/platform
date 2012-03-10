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
import scalaz.effect._
import scalaz.syntax.monad._

trait BinaryProjectionSerialization extends FileSerialization[Vector[SEvent]] {
  case class Header(idCount: Int, structure: Seq[(JPath, ColumnType)])
  final val HeaderFlag = 0
  final val EventFlag = 1

  def chunkSize: Int
  
  def writeElement(out: DataOutputStream, ev: Vector[SEvent]): IO[Unit] = {
    val (_, result) = ev.foldLeft((Option.empty[Header], IO(out.writeInt(ev.size)))) {
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

  def writeHeader(data: DataOutputStream, header: Header): IO[Unit] = IO {
    data.writeInt(HeaderFlag)
    data.writeInt(header.idCount)
    data.writeInt(header.structure.size)
    header.structure map {
      case (sel, valType) => {       
        data.writeUTF(sel.toString)
        data.writeUTF(nameOf(valType))
      }
    }
  }

  def readHeader(data: DataInputStream): IO[Header] = {
    def loop(data: DataInputStream, acc: Seq[(JPath, ColumnType)], i: Int): Seq[(JPath, ColumnType)] = {
      if (i > 0) {
        val selector = JPath(data.readUTF())
        ColumnType.fromName(data.readUTF()) match {
          case Some(ctype) => loop(data, acc :+ ((selector, ctype)), i - 1)
          case None        => sys.error("Memoization header corrupt: unable to read back column type indicator.")
        }
      } else {
        acc
      }
    }

    IO {
      val idCount = data.readInt()
      val columns = data.readInt()
      Header(idCount, loop(data, Vector.empty[(JPath, ColumnType)], columns)) 
    }
  }

  def writeEvent(data: DataOutputStream, ev: SEvent): IO[Unit] = {
    for {
      _ <- IO { data.writeInt(EventFlag) }
      _ <- writeIdentities(data, ev._1)
      _ <- writeValue(data, ev._2)
    } yield ()
  }

  def writeIdentities(data: DataOutputStream, id: Identities): IO[Unit] = IO {
    id.map(data.writeLong(_))
  }

  def writeValue(data: DataOutputStream, sv: SValue): IO[Unit] = {
    def determineValue(data: DataOutputStream, sv: SValue): Unit = {
      sv.fold(
        obj = obj       => obj.map { 
          case (_, v)   => determineValue(data, v)
        },
        arr = arr       => arr.map(v => determineValue(data, v)),
        str = str       => data.writeUTF(str),
        bool = bool     => data.writeBoolean(bool),
        long = long     => data.writeLong(long),
        double = double => data.writeDouble(double),
        num = num       => {
          val bytes = num.as[Array[Byte]]
          data.writeInt(bytes.length)
          data.write(bytes, 0, bytes.length)
        },
        nul = data.writeInt(0))
    }
    IO(determineValue(data, sv))
  }

  def readEvent(data: DataInputStream, length: Int, cols: Seq[(JPath, ColumnType)]): IO[SEvent] = {
    for {
      ids <- readIdentities(data, length)
      sv  <- readValue(data, cols)
    } yield (ids, sv)
  }

  def readIdentities(data: DataInputStream, length: Int): IO[Identities] = {
    def loop(data: DataInputStream, acc: VectorCase[Long], i: Int): Identities = {
      if (i > 0) {
        loop(data, acc :+ data.readLong(), i - 1)
      } else {
        acc
      }
    }

    IO {
      loop(data, VectorCase.empty[Long], length)
    }
  }

  def readValue(data: DataInputStream, cols: Seq[(JPath, ColumnType)]): IO[SValue] = IO {
    cols.foldLeft(Option.empty[SValue]) {
      case (None     , (JPath.Identity, ctype)) => Some(readColumn(data, ctype).toSValue)
      case (None     , (jpath, ctype))          => 
        jpath.nodes match {
          case JPathIndex(_) :: xs => SArray(Vector()).set(jpath, readColumn(data, ctype))
          case JPathField(_) :: xs => SObject(Map()).set(jpath, readColumn(data, ctype))
        }
      case (Some(obj), (JPath.Identity, ctype)) => sys.error("Illegal data header: multiple values at a selector root.")
      case (Some(obj), (jpath, ctype))          => obj.set(jpath, readColumn(data, ctype))
    } getOrElse {
      SNull
    }
  }

  private def readColumn(data: DataInputStream, ctype: ColumnType): CValue = ctype match {
    case SStringFixed(_)   => CString(data.readUTF())
    case SStringArbitrary  => CString(data.readUTF())
    case SBoolean          => CBoolean(data.readBoolean())
    case SInt              => CInt(data.readInt())
    case SLong             => CLong(data.readLong())
    case SFloat            => CFloat(data.readFloat())
    case SDouble           => CDouble(data.readDouble())
    case SDecimalArbitrary => 
      val length = data.readInt()
      val sdecimalarb: Array[Byte] = new Array(length)
      data.read(sdecimalarb)
      CNum(sdecimalarb.as[BigDecimal])
    case SNull                  => {
      data.readInt()
      CNull
    }
    case SEmptyObject           => CEmptyObject
    case SEmptyArray            => CEmptyArray
  }
}

