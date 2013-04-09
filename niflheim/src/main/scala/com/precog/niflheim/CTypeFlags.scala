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

