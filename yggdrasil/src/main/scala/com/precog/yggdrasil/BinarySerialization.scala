package com.precog
package yggdrasil

import leveldb._
import com.precog.analytics.Path

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

  def writeElement(out: DataOutputStream, ev: Vector[SEvent]): IO[Unit] = IO {
    out.writeInt(ev.size)
    ev.foldLeft((Option.empty[Header], IO(()))) {
      case ((oldHeader, io), (ids, sv)) => 
        val newHeader = Header(ids.size, sv.structure)
        val nextIO = if (oldHeader.forall(_ == newHeader)) {
          io >> writeEvent(out, (ids, sv))
        } else {
          io >> writeHeader(out, newHeader) >> writeEvent(out, (ids, sv))
        }

        (Option(newHeader), nextIO)
    }
  }
    
  def readElement(in: DataInputStream): IO[Option[Vector[SEvent]]] = {
    def loop(in: DataInputStream, acc: Vector[SEvent], i: Int, header: Option[Header]): IO[Option[Vector[SEvent]]] = { 
      in.readInt() match {
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
    }

    for {
      length <- IO { in.readInt() }
      result <- loop(in, Vector.empty[SEvent], length, None)
    } yield result
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

  def writeValue(data: DataOutputStream, sv: SValue): IO[Unit] = IO {
    sv.fold(
      obj = obj       => obj.map { 
        case (_, v)   => writeValue(data, v)
      },
      arr = arr       => arr.map(v => writeValue(data, v)),
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
    case SNull                  => CNull
    case SEmptyObject           => CEmptyObject
    case SEmptyArray            => CEmptyArray
  }
}
>>>>>>> master
