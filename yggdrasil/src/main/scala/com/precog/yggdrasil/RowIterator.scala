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

import java.nio._
import scala.annotation.tailrec
import scalaz.Ordering
import scalaz.Ordering._

trait RowIterator { self =>
  def idCount: Int
  def valueCount: Int 
  def structure: Seq[(CPath, CType)]

  def advance(i: Int): Boolean

  private[yggdrasil] def retreat(i: Int): Boolean

  def stringAt(i: Int): String
  def boolAt(i: Int): Boolean
  def intAt(i: Int): Int
  def longAt(i: Int): Long
  def floatAt(i: Int): Float
  def doubleAt(i: Int): Double
  def numAt(i: Int): BigDecimal

  def valueAt(i: Int): CValue
  def idAt(i: Int): Long
  def typeAt(i: Int): CType

  def append(iter: RowIterator) = new RefRowIterator {
    var ref = self
    private var pastCutover = 0
    override def advance(i: Int) = {
      @tailrec def innerAdvance(i: Int): Boolean = {
        (i == 0) || {
          if (ref.advance(1)) {
            if (ref eq iter) {
              pastCutover += 1
            }
            innerAdvance(i - 1)
          } else {
            if (ref != iter) {
              ref = iter
              innerAdvance(i)
            } else {
              false
            }
          }
        }
      }
      
      innerAdvance(i)
    }

    override private[yggdrasil] def retreat(i: Int) = {
      if (pastCutover > 0 && i >= pastCutover) {
        ref.retreat(pastCutover);
        ref = self
        val ret = ref.retreat(i - pastCutover);
        pastCutover = 0
        ret
      } else {
        if (pastCutover > 0) pastCutover -= i
        ref.retreat(i)
      }
    }
  }

  def apply(i: Int, cf: CFunction1): RowIterator = {
    (structure(i)._2, cf) match {
      case (CString, f: StringCF1) => StringRowIterator(self, f, i)
      case (CBoolean, f: BoolCF1) => BoolRowIterator(self, f, i)
      case (CInt, f: IntCF1) => IntRowIterator(self, f, i)
      case (CLong, f: LongCF1) => LongRowIterator(self, f, i)
      case (CFloat, f: FloatCF1) => FloatRowIterator(self, f, i)
      case (CDouble, f: DoubleCF1) => DoubleRowIterator(self, f, i)
      case (CNum, f: NumCF1) => NumRowIterator(self, f, i)
      case (_, f: StringPCF1) => StringPRowIterator(self, f, i)
      case (_, f: BoolPCF1) => BoolPRowIterator(self, f, i)
      case (_, f: IntPCF1) => IntPRowIterator(self, f, i)
      case (_, f: LongPCF1) => LongPRowIterator(self, f, i)
      case (_, f: FloatPCF1) => FloatPRowIterator(self, f, i)
      case (_, f: DoublePCF1) => DoublePRowIterator(self, f, i)
      case (_, f: NumPCF1) => NumPRowIterator(self, f, i)
    }
  }

  def apply(i: Int, j: Int, cpath: CPath, cf: CFunction2): RowIterator = {
    (structure(i)._2, structure(j)._2, cf) match {
      case (CString, CString, f: StringCF2) => StringApplyIterator(self, cpath, f, i, j)
      case (CBoolean, CBoolean, f: BoolCF2) => BoolApplyIterator(self, cpath, f, i, j)
      case (CInt, CInt, f: IntCF2) => IntApplyIterator(self, cpath, f, i, j)
      case (CLong, CLong, f: LongCF2) => LongApplyIterator(self, cpath, f, i, j)
      case (CFloat, CFloat, f: FloatCF2) => FloatApplyIterator(self, cpath, f, i, j)
      case (CDouble, CDouble, f: DoubleCF2) => DoubleApplyIterator(self, cpath, f, i, j)
      case (CNum, CNum, f: NumCF2) => NumApplyIterator(self, cpath, f, i, j)
      case (_, _, f: StringPCF2) => StringPApplyIterator(self, cpath, f, i, j)
      case (_, _, f: BoolPCF2) => BoolPApplyIterator(self, cpath, f, i, j)
      case (_, _, f: IntPCF2) => IntPApplyIterator(self, cpath, f, i, j)
      case (_, _, f: LongPCF2) => LongPApplyIterator(self, cpath, f, i, j)
      case (_, _, f: FloatPCF2) => FloatPApplyIterator(self, cpath, f, i, j)
      case (_, _, f: DoublePCF2) => DoublePApplyIterator(self, cpath, f, i, j)
      case (_, _, f: NumPCF2) => NumPApplyIterator(self, cpath, f, i, j)
    }
  }
}

abstract class BufferRowIterator[B <: Buffer](val ctype: CType) extends RowIterator {
  def keys: LongBuffer
  def values: B
  def selector: CPath
  def remaining = values.remaining

  private final val startPosition: Int = keys.position

  final val idCount = 1
  final val valueCount = 1
  lazy val structure = Seq((selector, ctype))

  final def advance(i: Int) = ((keys.position + i) < keys.limit) && ((values.position + i) < values.limit) && {
    keys.position(keys.position + i)
    values.position(values.position + i)
    true
  }

  final private[yggdrasil] def retreat(i: Int) = ((keys.position - i) > startPosition) && {
    keys.position(keys.position - i)
    values.position(values.position - i)
    true
  }

  final def idAt(i: Int): Long = keys.get(keys.position)
  final def typeAt(i: Int): CType = ctype
  @inline final protected def typeError(t: CType) = throw new ClassCastException(ctype + " cannot be coerced to a " + t)
}

case class BoolBufferRowIterator(keys: LongBuffer, values: ByteBuffer, selector: CPath) extends BufferRowIterator[ByteBuffer](CBoolean) {
  final def valueAt(i: Int): CValue = CBoolean(boolAt(i))

  final def stringAt(i: Int): String = boolAt(i).toString
  @inline final def boolAt(i: Int): Boolean = { assert(i == 0); values.get(values.position) != (0x00: Byte)}
  final def intAt(i: Int): Int = typeError(CInt)
  final def longAt(i: Int): Long = typeError(CLong)
  final def floatAt(i: Int): Float = typeError(CFloat)
  final def doubleAt(i: Int): Double = typeError(CDouble)
  final def numAt(i: Int): BigDecimal = typeError(CNum)
}

case class IntBufferRowIterator(keys: LongBuffer, values: IntBuffer, selector: CPath) extends BufferRowIterator[IntBuffer](CInt) {
  final def valueAt(i: Int): CValue = CInt(intAt(i))

  final def stringAt(i: Int): String = intAt(i).toString
  final def boolAt(i: Int): Boolean = typeError(CBoolean)
  @inline final def intAt(i: Int): Int = { assert(i == 0); values.get(values.position) }
  final def longAt(i: Int): Long = intAt(i)
  final def floatAt(i: Int): Float = intAt(i)
  final def doubleAt(i: Int): Double = intAt(i)
  final def numAt(i: Int) = BigDecimal(intAt(i))
}

case class LongBufferRowIterator(keys: LongBuffer, values: LongBuffer, selector: CPath) extends BufferRowIterator[LongBuffer](CLong) {
  final def valueAt(i: Int): CValue = CLong(longAt(i))

  final def stringAt(i: Int): String = longAt(i).toString
  final def boolAt(i: Int): Boolean = typeError(CBoolean)
  final def intAt(i: Int): Int = typeError(CInt)
  @inline final def longAt(i: Int): Long = { assert(i == 0); values.get(values.position) }
  final def floatAt(i: Int): Float = longAt(i)
  final def doubleAt(i: Int): Double = longAt(i)
  final def numAt(i: Int) = BigDecimal(longAt(i))
}

trait RefRowIterator extends RowIterator {
  def ref: RowIterator 

  final def idCount: Int = ref.idCount
  final def structure: Seq[(CPath, CType)] = ref.structure
  final def valueCount = ref.valueCount

  def advance(i: Int) = ref.advance(i)
  private[yggdrasil] def retreat(i: Int) = ref.retreat(i)

  final def valueAt(i: Int): CValue = ref.valueAt(i)
  final def idAt(i: Int): Long = ref.idAt(i)
  final def typeAt(i: Int): CType = ref.typeAt(i)

  def stringAt(i: Int): String = ref.stringAt(i)
  def boolAt(i: Int): Boolean = ref.boolAt(i)
  def intAt(i: Int): Int = ref.intAt(i)
  def longAt(i: Int): Long = ref.longAt(i)
  def floatAt(i: Int): Float = ref.floatAt(i)
  def doubleAt(i: Int): Double = ref.doubleAt(i)
  def numAt(i: Int): BigDecimal = ref.numAt(i)
}

abstract class ApplyRowIterator(selector: CPath, ctype: CType) extends RowIterator {
  def ref: RowIterator 

  final def idCount: Int = ref.idCount
  final val structure: Seq[(CPath, CType)] = Vector((selector, ctype))
  final def valueCount = ref.valueCount

  def advance(i: Int) = ref.advance(i)
  private[yggdrasil] def retreat(i: Int) = ref.retreat(i)

  final def idAt(i: Int): Long = ref.idAt(i)
  final def typeAt(i: Int): CType = ctype

  def valueAt(i: Int): CValue = sys.error("invalid dispatch")
  def stringAt(i: Int): String = sys.error("invalid dispatch")
  def boolAt(i: Int): Boolean = sys.error("invalid dispatch")
  def intAt(i: Int): Int = sys.error("invalid dispatch")
  def longAt(i: Int): Long = sys.error("invalid dispatch")
  def floatAt(i: Int): Float = sys.error("invalid dispatch")
  def doubleAt(i: Int): Double = sys.error("invalid dispatch")
  def numAt(i: Int): BigDecimal = sys.error("invalid dispatch")
}

case class StringRowIterator(ref: RowIterator, f: StringCF1, mappedCol: Int) extends RefRowIterator {
  override def stringAt(i: Int): String = 
    if (i == mappedCol) f.applyString(ref.stringAt(i)) else ref.stringAt(i)
}

case class StringPRowIterator(ref: RowIterator, f: StringPCF1, mappedCol: Int) extends RefRowIterator {
  override def stringAt(i: Int): String = 
    if (i == mappedCol) f(ref.valueAt(i)) else ref.stringAt(i)
}

case class StringApplyIterator(ref: RowIterator, selector: CPath, f: StringCF2, arg1: Int, arg2: Int) extends ApplyRowIterator(selector, CString) {
  override def stringAt(i: Int): String = {
    assert(i == 0)
    f.applyString(ref.stringAt(arg1), ref.stringAt(arg2)) 
  }
}

case class StringPApplyIterator(ref: RowIterator, selector: CPath, f: StringPCF2, arg1: Int, arg2: Int) extends ApplyRowIterator(selector, CString) {
  override def stringAt(i: Int): String = {
    assert(i == 0)
    f(ref.valueAt(arg1), ref.valueAt(arg2)) 
  }
}


case class BoolRowIterator(ref: RowIterator, f: BoolCF1, mappedCol: Int) extends RefRowIterator {
  override def boolAt(i: Int): Boolean = 
    if (i == mappedCol) f.applyBool(ref.boolAt(i)) else ref.boolAt(i)
}

case class BoolPRowIterator(ref: RowIterator, f: BoolPCF1, mappedCol: Int) extends RefRowIterator {
  override def boolAt(i: Int): Boolean = 
    if (i == mappedCol) f(ref.valueAt(i)) else ref.boolAt(i)
}

case class BoolApplyIterator(ref: RowIterator, selector: CPath, f: BoolCF2, arg1: Int, arg2: Int) extends ApplyRowIterator(selector, CBoolean) {
  override def boolAt(i: Int): Boolean = {
    assert(i == 0)
    f.applyBool(ref.boolAt(arg1), ref.boolAt(arg2)) 
  }
}

case class BoolPApplyIterator(ref: RowIterator, selector: CPath, f: BoolPCF2, arg1: Int, arg2: Int) extends ApplyRowIterator(selector, CBoolean) {
  override def boolAt(i: Int): Boolean = {
    assert(i == 0)
    f(ref.valueAt(arg1), ref.valueAt(arg2)) 
  }
}


case class IntRowIterator(ref: RowIterator, f: IntCF1, mappedCol: Int) extends RefRowIterator {
  override def intAt(i: Int): Int = 
    if (i == mappedCol) f.applyInt(ref.intAt(i)) else ref.intAt(i)
}

case class IntPRowIterator(ref: RowIterator, f: IntPCF1, mappedCol: Int) extends RefRowIterator {
  override def intAt(i: Int): Int = 
    if (i == mappedCol) f(ref.valueAt(i)) else ref.intAt(i)
}

case class IntApplyIterator(ref: RowIterator, selector: CPath, f: IntCF2, arg1: Int, arg2: Int) extends ApplyRowIterator(selector, CInt) {
  override def intAt(i: Int): Int = {
    assert(i == 0)
    f.applyInt(ref.intAt(arg1), ref.intAt(arg2)) 
  }
}

case class IntPApplyIterator(ref: RowIterator, selector: CPath, f: IntPCF2, arg1: Int, arg2: Int) extends ApplyRowIterator(selector, CInt) {
  override def intAt(i: Int): Int = {
    assert(i == 0)
    f(ref.valueAt(arg1), ref.valueAt(arg2)) 
  }
}


case class LongRowIterator(ref: RowIterator, f: LongCF1, mappedCol: Long) extends RefRowIterator {
  override def longAt(i: Int): Long = 
    if (i == mappedCol) f.applyLong(ref.longAt(i)) else ref.longAt(i)
}

case class LongPRowIterator(ref: RowIterator, f: LongPCF1, mappedCol: Long) extends RefRowIterator {
  override def longAt(i: Int): Long = 
    if (i == mappedCol) f(ref.valueAt(i)) else ref.longAt(i)
}

case class LongApplyIterator(ref: RowIterator, selector: CPath, f: LongCF2, arg1: Int, arg2: Int) extends ApplyRowIterator(selector, CLong) {
  override def longAt(i: Int): Long = {
    assert(i == 0)
    f.applyLong(ref.longAt(arg1), ref.longAt(arg2)) 
  }
}

case class LongPApplyIterator(ref: RowIterator, selector: CPath, f: LongPCF2, arg1: Int, arg2: Int) extends ApplyRowIterator(selector, CLong) {
  override def longAt(i: Int): Long = {
    assert(i == 0)
    f(ref.valueAt(arg1), ref.valueAt(arg2)) 
  }
}


case class FloatRowIterator(ref: RowIterator, f: FloatCF1, mappedCol: Float) extends RefRowIterator {
  override def floatAt(i: Int): Float = 
    if (i == mappedCol) f.applyFloat(ref.floatAt(i)) else ref.floatAt(i)
}

case class FloatPRowIterator(ref: RowIterator, f: FloatPCF1, mappedCol: Float) extends RefRowIterator {
  override def floatAt(i: Int): Float = 
    if (i == mappedCol) f(ref.valueAt(i)) else ref.floatAt(i)
}

case class FloatApplyIterator(ref: RowIterator, selector: CPath, f: FloatCF2, arg1: Int, arg2: Int) extends ApplyRowIterator(selector, CFloat) {
  override def floatAt(i: Int): Float = {
    assert(i == 0)
    f.applyFloat(ref.floatAt(arg1), ref.floatAt(arg2)) 
  }
}

case class FloatPApplyIterator(ref: RowIterator, selector: CPath, f: FloatPCF2, arg1: Int, arg2: Int) extends ApplyRowIterator(selector, CFloat) {
  override def floatAt(i: Int): Float = {
    assert(i == 0)
    f(ref.valueAt(arg1), ref.valueAt(arg2)) 
  }
}


case class DoubleRowIterator(ref: RowIterator, f: DoubleCF1, mappedCol: Double) extends RefRowIterator {
  override def doubleAt(i: Int): Double = 
    if (i == mappedCol) f.applyDouble(ref.doubleAt(i)) else ref.doubleAt(i)
}

case class DoublePRowIterator(ref: RowIterator, f: DoublePCF1, mappedCol: Double) extends RefRowIterator {
  override def doubleAt(i: Int): Double = 
    if (i == mappedCol) f(ref.valueAt(i)) else ref.doubleAt(i)
}

case class DoubleApplyIterator(ref: RowIterator, selector: CPath, f: DoubleCF2, arg1: Int, arg2: Int) extends ApplyRowIterator(selector, CDouble) {
  override def doubleAt(i: Int): Double = {
    assert(i == 0)
    f.applyDouble(ref.doubleAt(arg1), ref.doubleAt(arg2)) 
  }
}

case class DoublePApplyIterator(ref: RowIterator, selector: CPath, f: DoublePCF2, arg1: Int, arg2: Int) extends ApplyRowIterator(selector, CDouble) {
  override def doubleAt(i: Int): Double = {
    assert(i == 0)
    f(ref.valueAt(arg1), ref.valueAt(arg2)) 
  }
}


case class NumRowIterator(ref: RowIterator, f: NumCF1, mappedCol: BigDecimal) extends RefRowIterator {
  override def numAt(i: Int): BigDecimal = 
    if (i == mappedCol) f.applyNum(ref.numAt(i)) else ref.numAt(i)
}

case class NumPRowIterator(ref: RowIterator, f: NumPCF1, mappedCol: BigDecimal) extends RefRowIterator {
  override def numAt(i: Int): BigDecimal = 
    if (i == mappedCol) f(ref.valueAt(i)) else ref.numAt(i)
}

case class NumApplyIterator(ref: RowIterator, selector: CPath, f: NumCF2, arg1: Int, arg2: Int) extends ApplyRowIterator(selector, CNum) {
  override def numAt(i: Int): BigDecimal = {
    assert(i == 0)
    f.applyNum(ref.numAt(arg1), ref.numAt(arg2)) 
  }
}

case class NumPApplyIterator(ref: RowIterator, selector: CPath, f: NumPCF2, arg1: Int, arg2: Int) extends ApplyRowIterator(selector, CNum) {
  override def numAt(i: Int): BigDecimal = {
    assert(i == 0)
    f(ref.valueAt(arg1), ref.valueAt(arg2)) 
  }
}


case class ZipRowIterator(left: RowIterator, right: RowIterator) extends RowIterator {
  final val idCount = left.idCount + right.idCount
  final val structure = left.structure ++ right.structure
  final val valueCount = left.valueCount + right.valueCount
  private val leftSize = left.valueCount

  final def advance(i: Int) = left.advance(i) && right.advance(i)
  final private[yggdrasil] def retreat(i: Int) = { left.retreat(i) ; right.retreat(i) }

  final def stringAt(i: Int): String  = if (i < leftSize) left.stringAt(i) else right.stringAt(i - leftSize)
  final def boolAt(i: Int): Boolean   = if (i < leftSize) left.boolAt(i)   else right.boolAt(i - leftSize)
  final def intAt(i: Int): Int        = if (i < leftSize) left.intAt(i)    else right.intAt(i - leftSize)
  final def longAt(i: Int): Long      = if (i < leftSize) left.longAt(i)   else right.longAt(i - leftSize)
  final def floatAt(i: Int): Float    = if (i < leftSize) left.floatAt(i)  else right.floatAt(i - leftSize)
  final def doubleAt(i: Int): Double  = if (i < leftSize) left.doubleAt(i) else right.doubleAt(i - leftSize)
  final def numAt(i: Int): BigDecimal = if (i < leftSize) left.numAt(i)    else right.numAt(i - leftSize)

  final def valueAt(i: Int): CValue   = if (i < leftSize) left.valueAt(i)  else right.valueAt(i - leftSize)
  final def idAt(i: Int): Long        = if (i < left.idCount) left.idAt(i)        else right.idAt(i - left.idCount)
  final def typeAt(i: Int): CType     = if (i < leftSize) left.typeAt(i)   else right.typeAt(i - leftSize)
}

case class JoinRowIterator(left: RowIterator, right: RowIterator, identityPrefix: Int) extends RowIterator {
  //constructor
  private var repeats = 0
  private var retreatRepeats = 0
  advanceLeft && (compareIds(0) == EQ || advance(1));

  final val idCount: Int = left.idCount + right.idCount - identityPrefix
  final val structure: Seq[(CPath, CType)] = left.structure ++ right.structure
  final val valueCount = structure.length

  private val leftSize = left.valueCount
  private val leftIdSize = left.idCount

  @tailrec final def compareIds(i: Int): Ordering = {
    if (i >= identityPrefix) EQ
    else if (left.idAt(i) < right.idAt(i)) LT
    else if (left.idAt(i) > right.idAt(i)) GT
    else compareIds(i + 1)
  }

  @tailrec private final def advanceLeft: Boolean = {
    (compareIds(0) != LT) || (left.advance(1) && advanceLeft)
  }

  @tailrec final def advance(i: Int): Boolean = {
    (i == 0) || {
      if (right.advance(1)) {
        compareIds(0) match {
          case EQ => repeats += 1; advance(i - 1)
          case LT => {
            right.retreat(repeats + 1); repeats = 0;
            if (left.advance(1)) {
              compareIds(0) match {
                case EQ => advance(i - 1)
                case LT => advanceLeft && (compareIds(0) == EQ || advance(i))
                case GT => advance(i)
              }
            } else false
          }
          case GT => advance(i)
        }
      } else false
    }
  }

//  @tailrec final def advance(i: Int): Boolean = {
//    (i == 0) || { 
//      right.advance(1) && {
//        compareIds(0) match {
//          case EQ => repeats += 1; advance(i - 1)
//          case LT => right.retreat(repeats + 1); repeats = 0; false
//          case GT => advance(i)
//        }
//      }
//    } || {
//      left.advance(1) && {
//        compareIds(0) match {
//          case EQ => advance(i - 1)
//          case LT => advanceLeft && (compareIds(0) == EQ || advance(i))
//          case GT => advance(i)
//        }
//      }
//    }
//  }

  @tailrec private final def retreatLeft: Boolean = {
    (compareIds(0) != GT) || (left.retreat(1) && retreatLeft)
  }

  @tailrec final def retreat(i: Int): Boolean = {
    (i == 0) || {
      if (right.retreat(1)) {
        compareIds(0) match {
          case EQ => retreatRepeats += 1; retreat(i - 1)
          case GT => {
            right.advance(retreatRepeats + 1); retreatRepeats = 0;
            if (left.retreat(1)) {
              compareIds(0) match {
                case EQ => retreat(i - 1)
                case GT => retreatLeft && (compareIds(0) == EQ || retreat(i))
                case LT => retreat(i)
              }
            } else false
          }
          case LT => retreat(i)
        }
      } else false
    }
  }

//  @tailrec final def retreat(i: Int): Boolean = {
//    (i == 0) || { 
//      right.retreat(1) && {
//        compareIds(0) match {
//          case EQ => retreatRepeats += 1; retreat(i - 1)
//          case GT => right.advance(retreatRepeats + 1); retreatRepeats = 0; false
//          case LT => retreat(i)
//        }
//      }
//    } || {
//      left.retreat(1) && {
//        compareIds(0) match {
//          case EQ => retreat(i - 1)
//          case GT => retreatLeft && (compareIds(0) == EQ || retreat(i))
//          case LT => retreat(i)
//        }
//      }
//    }
//  }

  def stringAt(i: Int): String  = if (i < leftSize) left.stringAt(i) else right.stringAt(i - leftSize)
  def boolAt(i: Int): Boolean   = if (i < leftSize) left.boolAt(i) else right.boolAt(i - leftSize)
  def intAt(i: Int): Int        = if (i < leftSize) left.intAt(i) else right.intAt(i - leftSize)
  def longAt(i: Int): Long      = if (i < leftSize) left.longAt(i) else right.longAt(i - leftSize)
  def floatAt(i: Int): Float    = if (i < leftSize) left.floatAt(i) else right.floatAt(i - leftSize)
  def doubleAt(i: Int): Double  = if (i < leftSize) left.doubleAt(i) else right.doubleAt(i - leftSize)
  def numAt(i: Int): BigDecimal = if (i < leftSize) left.numAt(i) else right.numAt(i - leftSize)

  def valueAt(i: Int): CValue = if (i < leftSize) left.valueAt(i) else right.valueAt(i - leftSize)
  def idAt(i: Int): Long = if (i < leftIdSize) left.idAt(i) else right.idAt(identityPrefix + (i - leftIdSize))
  def typeAt(i: Int): CType = if (i < leftSize) left.typeAt(i) else right.typeAt(i - leftSize)
}
