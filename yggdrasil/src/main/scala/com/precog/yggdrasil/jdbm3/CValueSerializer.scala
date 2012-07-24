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

object CValueSerializer extends CValueSerializer {
  final val serialVersionUID = 20120724l
}

class CValueSerializer extends Serializer[Seq[CValue]] with Serializable {
  def readResolve() = CValueSerializer

  @transient
  val logger = Logger(classOf[CValueSerializer])

  // We use JDBM3's more efficient serializer for some basic types like Long, Double, and BigDecimal
  @transient
  private[this] final val delegate = new Serialization()

  // Flag to indicate CValue type. DO NOT EDIT if you don't know what you're doing
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
      out.writeInt(seq.size)
      seq.foreach { v => {
        out.writeByte(flagFor(v))

        v match {
          case CString(v)   => out.writeUTF(v)
          case CBoolean(v)  => out.writeBoolean(v)
          case CLong(v)     => delegate.serialize(out, v.asInstanceOf[java.lang.Long])
          case CDouble(v)   => delegate.serialize(out, v.asInstanceOf[java.lang.Double])
          case CNum(v)      => delegate.serialize(out, v.bigDecimal)
          case CDate(v)     => delegate.serialize(out, v.getMillis.asInstanceOf[java.lang.Long])
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
