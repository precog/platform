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
package com.precog.niflheim

import com.precog.common._

import com.precog.util.PrecogUnit

import java.io.IOException
import java.nio.ByteBuffer

import scala.annotation.tailrec
import scala.collection.mutable

import scalaz.{ Validation, Success, Failure }

object CTypeFlags {
  object Flags {
    val FBoolean: Byte = 1
    val FString: Byte = 2
    val FLong: Byte = 3
    val FDouble: Byte = 4
    val FBigDecimal: Byte = 5
    val FDate: Byte = 6
    val FArray: Byte = 7
    val FNull: Byte = 16
    val FEmptyArray: Byte = 17
    val FEmptyObject: Byte = 18
  }

  def getFlagFor(ctype: CType): Array[Byte] = {
    import Flags._

    val buffer = new mutable.ArrayBuffer[Byte]()

    def flagForCType(t: CType) {
      @tailrec
      def flagForCValueType(t: CValueType[_]) {
        t match {
          case CString => buffer += FString
          case CBoolean => buffer += FBoolean
          case CLong => buffer += FLong
          case CDouble => buffer += FDouble
          case CNum => buffer += FBigDecimal
          case CDate => buffer += FDate
          case CArrayType(tpe) =>
            buffer += FArray
            flagForCValueType(tpe)
        }
      }

      t match {
        case t: CValueType[_] =>
          flagForCValueType(t)
        case CNull =>
          buffer += FNull
        case CEmptyArray =>
          buffer += FEmptyArray
        case CEmptyObject =>
          buffer += FEmptyObject
        case CUndefined =>
          sys.error("Unexpected CUndefined type. Undefined segments don't exist!")
      }
    }

    flagForCType(ctype)
    buffer.toArray
  }

  def cTypeForFlag(flag: Array[Byte]): CType =
    readCType(ByteBuffer.wrap(flag)).fold(throw _, identity)

  def readCType(buffer: ByteBuffer): Validation[IOException, CType] = {
    import Flags._

    def readCValueType(flag: Byte): Validation[IOException, CValueType[_]] = flag match {
      case FBoolean => Success(CBoolean)
      case FString => Success(CString)
      case FLong => Success(CLong)
      case FDouble => Success(CDouble)
      case FBigDecimal => Success(CNum)
      case FDate => Success(CDate)
      case FArray => readCValueType(buffer.get()) map (CArrayType(_))
      case flag => Failure(new IOException("Unexpected segment type flag: %x" format flag))
    }

    buffer.get() match {
      case FNull => Success(CNull)
      case FEmptyArray => Success(CEmptyArray)
      case FEmptyObject => Success(CEmptyObject)
      case flag => readCValueType(flag)
    }
  }
}

