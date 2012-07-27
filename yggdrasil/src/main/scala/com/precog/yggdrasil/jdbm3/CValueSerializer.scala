package com.precog.yggdrasil
package jdbm3

import com.weiglewilczek.slf4s.Logger

import org.apache.jdbm._
import org.joda.time.DateTime

import java.io.{DataInput,DataOutput}

object CValueSerializer {
  def apply(format: Seq[CType]) = new CValueSerializer(format.toArray)
}

object CValueSerializerUtil {
  // We use JDBM3's more efficient serializer for some basic types like Long, Double, and BigDecimal
  private[jdbm3] val defaultSerializer = new Serialization()
  private[jdbm3] val logger = Logger(classOf[CValueSerializer])
}

class CValueSerializer private[CValueSerializer] (val format: Array[CType]) extends Serializer[Array[CValue]] with Serializable {
  final val serialVersionUID = 20120727l

  import CValueSerializerUtil._

  def serialize(out: DataOutput, seq: Array[CValue]) {
    try {
      var i = 0

      while (i < format.length) {
        format(i) match {
          case CString   => out.writeUTF(seq(i).asInstanceOf[CString].value)
          case CBoolean  => out.writeBoolean(seq(i).asInstanceOf[CBoolean].value)
          case CLong     => defaultSerializer.serialize(out, seq(i).asInstanceOf[CLong].value)
          case CDouble   => defaultSerializer.serialize(out, seq(i).asInstanceOf[CDouble].value.asInstanceOf[java.lang.Double])
          case CNum      => defaultSerializer.serialize(out, seq(i).asInstanceOf[CNum].value.bigDecimal)
          case CDate     => defaultSerializer.serialize(out, seq(i).asInstanceOf[CDate].value.getMillis.asInstanceOf[java.lang.Long])
          case CNull | CEmptyObject | CEmptyArray => // No value to serialize
        }
        i += 1
      }
    } catch {
      case t: Throwable => logger.error("Error during serialization", t)
    }
  }

  def deserialize(in: DataInput): Array[CValue] = {
    val values = new Array[CValue](format.length)
    
    var i = 0
    while (i < format.length) {
      values(i) = format(i) match {
        case CString      => CString(in.readUTF())
        case CBoolean     => CBoolean(in.readBoolean())
        case CLong        => CLong(defaultSerializer.deserialize(in).asInstanceOf[java.lang.Long])
        case CDouble      => CDouble(defaultSerializer.deserialize(in).asInstanceOf[java.lang.Double])
        case CNum         => CNum(BigDecimal(defaultSerializer.deserialize(in).asInstanceOf[java.math.BigDecimal]))
        case CDate        => CDate(new DateTime(defaultSerializer.deserialize(in).asInstanceOf[java.lang.Long]))
        case CNull        => CNull
        case CEmptyObject => CEmptyObject
        case CEmptyArray  => CEmptyArray
      }
      i += 1
    }

    values
  }
}
