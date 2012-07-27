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
