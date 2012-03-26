package com.precog.yggdrasil
package serialization

import bijections._

import com.precog.common.VectorCase
import com.precog.yggdrasil.SValue._
import com.precog.yggdrasil.ColumnType._
import com.precog.util.Bijection._

import blueeyes.json._
import blueeyes.json.JPath._

import java.io._
import scala.annotation.tailrec
import scalaz.effect._
import scalaz.syntax.monad._


trait SValueFormatting {
  def writeStructure(out: DataOutputStream, structure: Seq[(JPath, ColumnType)]): Unit
  def readStructure(in: DataInputStream): Seq[(JPath, ColumnType)]

  def writeValue(out: DataOutputStream, sv: SValue): Unit
  def readValue(in: DataInputStream, cols: Seq[(JPath, ColumnType)]): SValue
}

trait IdentitiesFormatting {
  def writeIdentities(out: DataOutputStream, id: Identities): Unit
  def readIdentities(in: DataInputStream, length: Int): Identities
}

trait BinarySValueFormatting extends SValueFormatting with IdentitiesFormatting {
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

  def writeIdentities(out: DataOutputStream, id: Identities): Unit = {
    id.foreach(out.writeLong(_))
  }

  def readIdentities(in: DataInputStream, length: Int): Identities = {
    @tailrec def loop(in: DataInputStream, acc: VectorCase[Long], i: Int): Identities = {
      if (i > 0) {
        loop(in, acc :+ in.readLong(), i - 1)
      } else {
        acc
      }
    }

    loop(in, VectorCase.empty[Long], length)
  }
}
// vim: set ts=4 sw=4 et:
