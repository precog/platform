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

class BinarySerialization {
  def writeHeader(data: DataOutputStream, col: Seq[(JPath, ColumnType)]): Unit = {
    data.writeInt(col.size)
    col collect {
      case (sel, valType) => {       
        val selectorString = sel.toString 
        val valueTypeString = nameOf(valType)
        
        data.writeUTF(selectorString)
        data.writeUTF(valueTypeString)
      }
    }
  }

  def readHeader(data: DataInputStream): Seq[(JPath, ColumnType)] = {   
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

  def sValueToBinary(data: DataOutputStream, sv: SValue): Unit = sv.fold(
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

  def binaryToSValue(data: DataInputStream, cols: Seq[(JPath, ColumnType)]): SValue = {
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
}
