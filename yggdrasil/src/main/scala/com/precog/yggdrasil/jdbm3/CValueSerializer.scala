package com.precog.yggdrasil
package jdbm3

import org.apache.jdbm._
import org.joda.time.DateTime

import java.io.{DataInput,DataOutput}

object CValueSerializer extends CValueSerializer {
  final val serialVersionUID = 20120724l
}

class CValueSerializer extends Serializer[Seq[CValue]] with Serializable {
  def readResolve() = CValueSerializer

  private[this] final val delegate = new Serialization()

  private[this] final def flagFor(c: CValue): Byte = c match {
    case c: CString      => 0.toByte   
    case c: CBoolean     => 1.toByte  
    case c: CLong        => 2.toByte     
    case c: CDouble      => 3.toByte   
    case c: CNum         => 4.toByte      
    case c: CDate        => 5.toByte
    case CNull           => 6.toByte
    case CEmptyObject    => 7.toByte
    case CEmptyArray     => 8.toByte
  }

  def serialize(out: DataOutput, seq: Seq[CValue]) {
    try {
      println("Serializing " + seq + " to " + out)
      out.writeInt(seq.size)
      seq.foreach { v => {
        out.writeByte(flagFor(v))

        v match {
          case CString(v)   => out.writeUTF(v)
          case CBoolean(v)  => out.writeBoolean(v)
          case CLong(v)     => delegate.serialize(out, v)
          case CDouble(v)   => delegate.serialize(out, v)
          case CNum(v)      => delegate.serialize(out, v)
          case CDate(v)     => delegate.serialize(out, v.getMillis)
          case CNull | CEmptyObject | CEmptyArray => // No value to serialize
        }
      }}

      println("Serialized to " + out)
    } catch {
      case t: Throwable => println("Error during serialization: " + t)
    }
  }

  def deserialize(in: DataInput): Seq[CValue] = {
    println("Deserializing Seq[CValue]")
    val length = in.readInt()
    val values = new Array[CValue](length)
    
    var i = 0
    while (i < length) {
      values(i) = in.readByte() match {
        case 0 => CString(in.readUTF())
        case 1 => CBoolean(in.readBoolean())
        case 2 => CLong(delegate.deserialize(in).asInstanceOf[java.lang.Long])
        case 3 => CDouble(delegate.deserialize(in).asInstanceOf[java.lang.Double])
        case 4 => CNum(BigDecimal(delegate.deserialize(in).asInstanceOf[java.math.BigDecimal]))
        case 5 => CDate(new DateTime(delegate.deserialize(in).asInstanceOf[java.lang.Long]))
        case 6 => CNull
        case 7 => CEmptyObject
        case 8 => CEmptyArray
      }
      i += 1
    }

    values
  }
}
