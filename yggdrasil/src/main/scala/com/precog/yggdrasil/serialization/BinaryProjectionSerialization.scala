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

