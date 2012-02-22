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
  def writeHeader(data: DataOutputStream, col: Set[(JPath, ColumnType)]): Unit = {
    col collect {
      case (sel, valType) => {       
        val selectorString = sel.toString 
        val valueTypeString = nameOf(valType)
        
        data.writeUTF(selectorString)
        data.writeUTF(valueTypeString)
      }
    }
  }

  implicit def stringToColumnType(str: String): ColumnType = str match {  
    case "String" => SStringArbitrary
    case "Boolean" => SBoolean
    case "Int" => SInt
    case "Long" => SLong
    case "Float" => SFloat
    case "Double" => SDouble
    case "Decimal" => SDecimalArbitrary
    case "Null" => SNull
    case "SEmptyObject" => SEmptyObject
    case "EmptyArray" => SEmptyArray
  }

  def readHeader(data: DataInputStream): Set[(JPath, ColumnType)] = {   //todo needs to return type Set
    def loop(data: DataInputStream, set: Set[(JPath, ColumnType)]): Set[(JPath, ColumnType)] = {
      try {
        val (data1, data2) = (JPath(data.readUTF()), data.readUTF())
        val newSet = set + ((data1, data2))

        loop(data, newSet)
      } catch {
        case e: IOException => 
          exit(1)
      }
      set
    }
    loop(data, Set.empty[(JPath, ColumnType)])
  }

  def sValueToBinary(data: DataOutputStream, sv: SValue): Unit = sv.fold(
    obj = obj       => obj.map { 
      case (k, v) => {
        data.writeUTF(k)
        sValueToBinary(data, v)
      }
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

  def colTypeToSValue(data: DataInputStream, column: ColumnType): SValue = column match {
    case SStringArbitrary    => SString(data.readUTF())
    case SStringFixed(w)     => SString(data.readUTF())
    case SBoolean            => SBoolean(data.readBoolean())
    case SInt                => SInt(data.readInt())
    case SLong               => SLong(data.readLong())
    case SFloat              => SFloat(data.readFloat())
    case SDouble             => SDouble(data.readDouble())
    case SDecimalArbitrary   => {
      val length = data.readInt()
      val sdecimalarb: Array[Byte] = new Array(length)
      data.read(sdecimalarb)
      SDecimal(sdecimalarb.as[BigDecimal])
    }
    case SNull               => SNull
    case SEmptyObject        => SEmptyObject
    case SEmptyArray         => SEmptyArray

  }

  def binaryToSValue(data: DataInputStream, tp: SType, col: Seq[ColumnType]): SValue = {
    tp match { 
      case SObject           => SObject(col.map {
        column => (data.readUTF(), colTypeToSValue(data, column))
      }.toMap)

      case SArray            => SArray(col.map {
        column => colTypeToSValue(data, column)
      }.foldLeft(Vector.empty[SValue])((vector, col) => vector :+ col))

      case SString           => SString(data.readUTF())
      case SBoolean          => SBoolean(data.readBoolean())
      case SInt              => SInt(data.readInt())
      case SLong             => SLong(data.readLong())
      case SFloat            => SFloat(data.readFloat())
      case SDouble           => SDouble(data.readDouble())
      case SDecimal          => {
        val length = data.readInt()
        val sdecimalarb: Array[Byte] = new Array(length)
        data.read(sdecimalarb)
        SDecimal(sdecimalarb.as[BigDecimal])
      }
    }
  }
}
