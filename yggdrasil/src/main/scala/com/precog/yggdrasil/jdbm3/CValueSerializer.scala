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
package jdbm3

import com.weiglewilczek.slf4s.Logger

import org.apache.jdbm._
import org.joda.time.DateTime

import java.io.{DataInput,DataOutput}

import scala.collection.BitSet

/* I really hate using this, but the custom serialization code in JDBM3 is choking in
 * very weird ways when using "CType" as the format type. It appears that it "registers"
 * the CType class in the serializer, but on read it blows up because the new serializer
 * hasn't registered the same class and I can't figure out how to properly register it.
 * Using Byte values to indicate CType is somewhat of a hack, but it works
 *
 * DCB - 2012-07-27
 */
private[jdbm3] object CTypeMappings {
  final val FSTRING      = 0.toByte
  final val FBOOLEAN     = 1.toByte
  final val FLONG        = 2.toByte
  final val FDOUBLE      = 3.toByte
  final val FNUM         = 4.toByte
  final val FDATE        = 5.toByte
  final val FNULL        = 6.toByte
  final val FEMPTYOBJECT = 7.toByte
  final val FEMPTYARRAY  = 8.toByte
  final val FUNDEFINED   = -1.toByte

  def flagFor(tpe: CType): Byte = tpe match {
    case CString      => FSTRING
    case CBoolean     => FBOOLEAN
    case CLong        => FLONG
    case CDouble      => FDOUBLE
    case CNum         => FNUM
    case CDate        => FDATE
    case CNull        => FNULL
    case CEmptyObject => FEMPTYOBJECT
    case CEmptyArray  => FEMPTYARRAY
    case CUndefined   => sys.error("Undefined is not a valid format")
  }

  def fromFlag(b: Byte): CType = b match {
    case FSTRING       => CString     
    case FBOOLEAN      => CBoolean    
    case FLONG         => CLong       
    case FDOUBLE       => CDouble     
    case FNUM          => CNum        
    case FDATE         => CDate       
    case FNULL         => CNull       
    case FEMPTYOBJECT  => CEmptyObject
    case FEMPTYARRAY   => CEmptyArray 
    case invalid       => sys.error(invalid + " is not a valid format")
  }
}

object CValueSerializer {
  import CTypeMappings._
  def apply(format: Seq[CType]) = new CValueSerializer(format.map(flagFor).toArray)
}

object CValueSerializerUtil {
  // We use JDBM3's more efficient serializer for some basic types like Long, Double, and BigDecimal
  private[jdbm3] val defaultSerializer = new Serialization()
  private[jdbm3] val logger = Logger(classOf[CValueSerializer])
}

class CValueSerializer private[CValueSerializer] (val format: Array[Byte]) extends Serializer[Array[CValue]] with Serializable {
  import CTypeMappings._

  @transient
  private final val emptyBytes = Array[Byte]()

  final val serialVersionUID = 20120730l

  import CValueSerializerUtil._

  /* The undefined bitset is encoded as an array of bytes backing the bitset (essentially
   * converting from an Array of longs to minimize required space) */
  def serializeUndefinedIndices(out: DataOutput, undefined: BitSet) {
    if (undefined.isEmpty) {
      defaultSerializer.serialize(out, emptyBytes)
    } else {
      val backingBytes = new Array[Byte](undefined.max / 8 + 1)

      undefined.foreach { 
        index => backingBytes(index / 8) = (backingBytes(index / 8) | (0x01 << (7 - (index % 8)))).toByte
      }

      defaultSerializer.serialize(out, backingBytes)
    }
  }

  def deserializeUndefinedIndices(in: DataInput): BitSet = {
    // Read in the byte array for undefined values
    var backingBytes = defaultSerializer.deserialize(in).asInstanceOf[Array[Byte]]

    if (backingBytes.length == 0) {
      BitSet.empty
    } else {
      var result = BitSet.empty

      var octet = 0
      while (octet < backingBytes.length) {
        var i = 7
        while (i >= 0) {
          if ((0x01 & (backingBytes(octet) >> i)) != 0) {
            result += (octet * 8 + (7 - i))
          }
          i -= 1
        }
        octet += 1
      }

      result
    }
  }

  def serialize(out: DataOutput, seq: Array[CValue]) {
    try {
      var i = 0

      var undefined = BitSet()

      while (i < format.length) {
        if (seq(i) == CUndefined) {
          undefined += i
        }
        i += 1
      }

      serializeUndefinedIndices(out, undefined)
     
      i = 0
      while (i < format.length) {
        if (undefined(i)) {
          out.write(0) // TODO: required?
        } else {
          format(i) match {
            case FSTRING   => out.writeUTF(seq(i).asInstanceOf[CString].value)
            case FBOOLEAN  => out.writeBoolean(seq(i).asInstanceOf[CBoolean].value)
            case FLONG     => defaultSerializer.serialize(out, seq(i).asInstanceOf[CLong].value)
            case FDOUBLE   => defaultSerializer.serialize(out, seq(i).asInstanceOf[CDouble].value.asInstanceOf[java.lang.Double])
            case FNUM      => {
              val backing: java.math.BigDecimal = seq(i).asInstanceOf[CNum].value.bigDecimal
              out.writeUTF(backing.toString)
            }
            case FDATE     => defaultSerializer.serialize(out, seq(i).asInstanceOf[CDate].value.getMillis.asInstanceOf[java.lang.Long])
            case FNULL | FEMPTYOBJECT | FEMPTYARRAY => out.write(0) // TODO: No value to serialize, but JDBM3 *requires* a value to be written. Can we avoid this?
            case invalid => sys.error("Invalid format flag: " + invalid)
          }
        }
        i += 1
      }
    } catch {
      case t: Throwable => logger.error("Error during serialization", t)
    }
  }

  def deserialize(in: DataInput): Array[CValue] = {
    val values = new Array[CValue](format.length)

    val undefined = deserializeUndefinedIndices(in)
    
    var i = 0
    while (i < format.length) {
      if (undefined(i)) {
        in.skipBytes(1)
        values(i) = CUndefined
      } else {
        values(i) = format(i) match {
          case FSTRING      => CString(in.readUTF())
          case FBOOLEAN     => CBoolean(in.readBoolean())
          case FLONG        => CLong(defaultSerializer.deserialize(in).asInstanceOf[java.lang.Long])
          case FDOUBLE      => CDouble(defaultSerializer.deserialize(in).asInstanceOf[java.lang.Double])
          case FNUM         => CNum(BigDecimal(new java.math.BigDecimal(in.readUTF())))
          case FDATE        => CDate(new DateTime(defaultSerializer.deserialize(in).asInstanceOf[java.lang.Long]))
          case FNULL        => in.skipBytes(1); CNull        // JDBM requires that we write a value, so we have to read one byte back
          case FEMPTYOBJECT => in.skipBytes(1); CEmptyObject // JDBM requires that we write a value, so we have to read one byte back 
          case FEMPTYARRAY  => in.skipBytes(1); CEmptyArray  // JDBM requires that we write a value, so we have to read one byte back
          case invalid      => sys.error("Invalid format flag: " + invalid)
        }
      }
      i += 1
    }

    values
  }
}
