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
package leveldb

import com.precog.analytics.Path

import com.precog.util.Bijection._

import com.precog.yggdrasil._
import com.precog.yggdrasil.SValue._
import com.precog.yggdrasil.ColumnType._

import blueeyes.json._
import blueeyes.json.JPath._

import scala.actors.remote._

import java.io._

trait BinaryProjectionSerialization extends FileSerialization[SEvent] {
  type Structure = (Int, Seq[(JPath, ColumnType)])
  final val HeaderFlag = 0
  final val EventFlag = 1

  def chunkSize: Int

  def writeEvents(out: DataOutputStream, ev: Vector[SEvent]): IO[Unit] {
    ev.foldLeft(Option.empty[Structure])
    // in a tail-recursive loop or fold over ev
    // compute the header for an event
    // if the header is the same as previously seen, just write the event
    // if the header is different, write the header then write the event
  }

  def readEvents(in: DataInputStream): IO[Option[Vector[SEvent]]] {
    // in a tail-recursive loop
    // first read a header
    // then read events using that header until you see another header or reach the maximum chunk size
  }

  def writeHeader(data: DataOutputStream, col: Seq[(JPath, ColumnType)]): IO[Unit] = IO {
    data.writeInt(HeaderFlag)
    data.writeInt(col.size)
    col map {
      case (sel, valType) => {       
        data.writeUTF(sel.toString)
        data.writeUTF(nameOf(valType))
      }
    }
  }

  def readHeader(data: DataInputStream): IO[Seq[(JPath, ColumnType)]] = IO {   
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

    loop(data, Vector.empty[(JPath, ColumnType)], data.readInt())
  }

  def writeEvent(data: DataOutputStream, ev: SEvent): IO[Unit] = IO {
    data.writeInt(EventFlag)
    writeIdentities(data, ev._1)
    writeValue(data, ev._2)
  }

  def writeValue(data: DataOutputStream, sv: SValue): IO[Unit] = IO {
    sv.fold(
      obj = obj       => obj.map { 
        case (_, v)   => sValueToBinary(data, v)
      },
      arr = arr       => arr.map(v => sValueToBinary(data, v)),
      str = str       => data.writeUTF(str),
      bool = bool     => data.writeBoolean(bool),
      long = long     => data.writeLong(long),
      double = double => data.writeDouble(double),
      num = num       => {
        val bytes = num.as[Array[Byte]]
        data.writeInt(bytes.length)
        data.write(bytes, 0, bytes.length)
      },
      nul = sys.error("nothing should be written") )
  }

  def readNext(data: DataInputStream, Option[Seq[(JPath, ColumnType)])]): IO[Either[Seq[(JPath, ColumnType)], SEvent]] = IO {
    data.readInt() match {
      case HeaderFlag => Left(readHeader(data))
      case EventFlag => Right(readEvent(data))
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
    case SNull                  => CNull
    case SEmptyObject           => CEmptyObject
    case SEmptyArray            => CEmptyArray
  }
}
