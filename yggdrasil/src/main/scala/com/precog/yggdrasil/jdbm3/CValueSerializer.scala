package com.precog.yggdrasil
package jdbm3

import com.weiglewilczek.slf4s.Logger

import org.apache.jdbm._
import org.joda.time.DateTime

import java.io.{DataInput,DataOutput}

object CValueSerializer extends CValueSerializer {
  final val serialVersionUID = 20120724l
}

object CValueSerializerUtil {
  // We use JDBM3's more efficient serializer for some basic types like Long, Double, and BigDecimal
  private[jdbm3] val defaultSerializer = new Serialization()
  private[jdbm3] val logger = Logger(classOf[CValueSerializer])
}

class CValueSerializer extends Serializer[Seq[CValue]] with Serializable {
  import CValueSerializerUtil._

  def readResolve() = CValueSerializer

  // Flag to indicate CValue type. DO NOT EDIT if you don't know what you're doing
  private[this] final def flagFor(c: CValue): Int = c match {
    case c: CString      => 0   
    case c: CBoolean     => 1  
    case c: CLong        => 2     
    case c: CDouble      => 3   
    case c: CNum         => 4      
    case c: CDate        => 5
    case CNull           => 6
    case CEmptyObject    => 7
    case CEmptyArray     => 8
  }

  def serialize(out: DataOutput, seq: Seq[CValue]) {
    try {
      out.writeInt(seq.size)
      seq.foreach { v => {
        out.write(flagFor(v))

        v match {
          case CString(v)   => out.writeUTF(v)
          case CBoolean(v)  => out.writeBoolean(v)
          case CLong(v)     => defaultSerializer.serialize(out, v.asInstanceOf[java.lang.Long])
          case CDouble(v)   => defaultSerializer.serialize(out, v.asInstanceOf[java.lang.Double])
          case CNum(v)      => defaultSerializer.serialize(out, v.bigDecimal)
          case CDate(v)     => defaultSerializer.serialize(out, v.getMillis.asInstanceOf[java.lang.Long])
          case CNull | CEmptyObject | CEmptyArray => // No value to serialize
        }
      }}
    } catch {
      case t: Throwable => logger.error("Error during serialization", t)
    }
  }

  def deserialize(in: DataInput): Seq[CValue] = {
    val length = in.readInt()
    val values = new Array[CValue](length)
    
    var i = 0
    while (i < length) {
      values(i) = in.readByte() match {
        case 0 => CString(in.readUTF())
        case 1 => CBoolean(in.readBoolean())
        case 2 => CLong(defaultSerializer.deserialize(in).asInstanceOf[java.lang.Long])
        case 3 => CDouble(defaultSerializer.deserialize(in).asInstanceOf[java.lang.Double])
        case 4 => CNum(BigDecimal(defaultSerializer.deserialize(in).asInstanceOf[java.math.BigDecimal]))
        case 5 => CDate(new DateTime(defaultSerializer.deserialize(in).asInstanceOf[java.lang.Long]))
        case 6 => CNull
        case 7 => CEmptyObject
        case 8 => CEmptyArray
      }
      i += 1
    }

    values
  }
}
