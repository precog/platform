package com.precog
package yggdrasil
package serialization

import bijections.bd2ab
import com.precog.common.VectorCase
import com.precog.yggdrasil.SValue._
import com.precog.yggdrasil.CType._
import com.precog.yggdrasil.leveldb._
import com.precog.util._
import Bijection._

import blueeyes.json._
import blueeyes.json.JPath._

import java.io._
import scala.annotation.tailrec
import scalaz.effect._
import scalaz.syntax.monad._



/*
trait BinaryProjectionSerialization extends IterateeFileSerialization[Vector[SEvent]] with BinarySValueFormatting {
  case class Header(idCount: Int, structure: Seq[(JPath, CType)])
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

  def readEvent(in: DataInputStream, length: Int, cols: Seq[(JPath, CType)]): IO[SEvent] = {
    for {
      ids <- IO { readIdentities(in, length) }
      sv  <- IO { readValue(in, cols) } 
    } yield (ids, sv)
  }
}
*/

