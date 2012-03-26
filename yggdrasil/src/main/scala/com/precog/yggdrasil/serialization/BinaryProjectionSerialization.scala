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

import bijections.bd2ab
import com.precog.common.VectorCase
import com.precog.yggdrasil.SValue._
import com.precog.yggdrasil.ColumnType._
import com.precog.yggdrasil.leveldb._
import com.precog.util._
import Bijection._

import blueeyes.json._
import blueeyes.json.JPath._

import java.io._
import scala.annotation.tailrec
import scalaz.effect._
import scalaz.syntax.monad._



trait BinaryProjectionSerialization extends IterateeFileSerialization[Vector[SEvent]] with BinarySValueFormatting {
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
      _ <- IO { writeIdentities(out, ev._1) }
      _ <- IO { writeValue(out, ev._2) }
    } yield ()
  }

  def readEvent(in: DataInputStream, length: Int, cols: Seq[(JPath, ColumnType)]): IO[SEvent] = {
    for {
      ids <- IO { readIdentities(in, length) }
      sv  <- IO { readValue(in, cols) } 
    } yield (ids, sv)
  }
}

