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

import table._
import com.precog.util._

import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._

import org.joda.time.DateTime

import scalaz._
import scalaz.Ordering._
import scalaz.syntax.order._
import scalaz.std._
import scalaz.std.math._
import scalaz.std.AllInstances._

import _root_.java.io.{Externalizable,ObjectInput,ObjectOutput}
import _root_.java.math.MathContext

sealed trait CValue {
  def cType: CType

  def toJValue: JValue
}

sealed trait CNullValue extends CValue { self: CNullType =>
  def cType: CNullType = self
}

sealed trait CWrappedValue[@specialized(Boolean, Long, Double) A] extends CValue {
  def cType: CValueType[A]
  def value: A
  def toJValue = cType.jValueFor(value)
}

sealed trait CNumericValue[A] extends CWrappedValue[A] {
  def cType: CNumericType[A]
  def toCNum: CNum = CNum(cType.bigDecimalFor(value))
}

object CValue {
  def compareValues(a: CValue, b: CValue): Int = (a,b) match {
    case (CString(as), CString(bs)) => as.compareTo(bs)
    case (CBoolean(ab), CBoolean(bb)) => ab.compareTo(bb)
    case (CLong(al), CLong(bl)) => al.compareTo(bl)
    case (CDouble(ad), CDouble(bd)) => ad.compareTo(bd)
    case (CNum(an), CNum(bn)) => an.compareTo(bn)
    case (CDate(ad), CDate(bd)) => ad.compareTo(bd)
    case (CArray(as, CArrayType(atpe)), CArray(bs, CArrayType(btpe))) if atpe == btpe =>
      (as.view zip bs.view) map { case (a, b) =>
        compareValues(atpe(a), btpe(b))
      } find (_ != 0) getOrElse (as.size - bs.size)

    // The only safe way to compare any mix of the 3 types.
    case (a: CNumericValue[_], b: CNumericValue[_]) =>
      compareValues(a.toCNum, b.toCNum)
        
    case (a: CNullValue, b: CNullValue) if a.cType == b.cType => 0

    case invalid                      => sys.error("Invalid comparison for SortingKey of " + invalid)
  }

  implicit object CValueOrder extends Order[CValue] {
    def order(a: CValue, b: CValue): Ordering = if (a.cType == b.cType) {
      Ordering.fromInt(compareValues(a, b))
    } else {
      CType.CTypeOrder.order(a.cType, b.cType)
    }
  }
}


sealed trait CType extends Serializable {
  def readResolve(): CType
  def format: StorageFormat
  def isNumeric: Boolean = false

  @inline 
  private[CType] final def typeIndex = this match {
    case CBoolean      => 0

    case CString       => 2
    
    case CLong         => 4
    case CDouble       => 6
    case CNum          => 7
    
    case CEmptyObject  => 8

    case CEmptyArray   => 9

    case CArrayType(_) => 10

    case CNull         => 11

    case CDate         => 12

    case CUndefined    => 13
  }
}

sealed trait CNullType extends CType with CNullValue {
  val format = FixedWidth(0)
}

sealed abstract class CValueType[A](val format: StorageFormat) extends CType { self =>
  def readResolve(): CValueType[A]
  def apply(a: A): CWrappedValue[A]
  def order(a: A, b: A): Ordering
  def jValueFor(a: A): JValue
}

sealed abstract class CNumericType[A](format: StorageFormat) extends CValueType[A](format) {
  override def isNumeric: Boolean = true
  def bigDecimalFor(a: A): BigDecimal
}

trait CTypeSerialization {
  def nameOf(c: CType): String = c match {
    case CString                => "String"
    case CBoolean               => "Boolean"
    case CLong                  => "Long"
    case CDouble                => "Double"
    case CNum                   => "Decimal"
    case CNull                  => "Null"
    case CEmptyObject           => "EmptyObject"
    case CEmptyArray            => "EmptyArray"
    case CArrayType(elemType)   => "Array[%s]" format nameOf(elemType)
    case CDate                  => "Timestamp"
    case CUndefined             => sys.error("CUndefined cannot be serialized")
  } 

  val ArrayName = """Array[(.*)]""".r

  def fromName(n: String): Option[CType] = n match {
    case "String"        => Some(CString)
    case "Boolean"       => Some(CBoolean)
    case "Long"          => Some(CLong)
    case "Double"        => Some(CDouble)
    case "Decimal"       => Some(CNum)
    case "Null"          => Some(CNull)
    case "EmptyObject"   => Some(CEmptyObject)
    case "EmptyArray"    => Some(CEmptyArray)
    case ArrayName(elemName) => fromName(elemName) flatMap {
      case elemType: CValueType[_] => Some(CArrayType(elemType))
      case _ => None
    }
    case "Timestamp"     => Some(CDate)
    case _ => None
  }
    
  implicit val CTypeDecomposer : Decomposer[CType] = new Decomposer[CType] {
    def decompose(ctype : CType) : JValue = JString(nameOf(ctype))
  }

  implicit val CTypeExtractor : Extractor[CType] = new Extractor[CType] with ValidatedExtraction[CType] {
    override def validated(obj : JValue) : Validation[Extractor.Error,CType] = 
      obj.validated[String].map( fromName _ ) match {
        case Success(Some(t)) => Success(t)
        case Success(None)    => Failure(Extractor.Invalid("Unknown type."))
        case Failure(f)       => Failure(f)
      }
  }
}

case object CType extends CTypeSerialization {
  def readResolve() = CType

  def of(v: CValue): CType = v.cType

  def canCompare(t1: CType, t2: CType): Boolean =
    (t1 == t2) || (t1.isNumeric && t2.isNumeric)

  def unify(t1: CType, t2: CType): Option[CType] = {
    (t1, t2) match {
      case (CLong, CLong)     => Some(CLong)
      case (CLong, CDouble)   => Some(CDouble)
      case (CLong, CNum)      => Some(CNum)
      case (CDouble, CLong)   => Some(CNum)
      case (CDouble, CDouble) => Some(CDouble)
      case (CDouble, CNum)    => Some(CNum)
      case (CNum, CLong)      => Some(CNum)
      case (CNum, CDouble)    => Some(CNum)
      case (CNum, CNum)       => Some(CNum)

      case (CString, CString) => Some(CString)

      case (CArrayType(et1), CArrayType(et2)) =>
        unify(et1, et2) flatMap {
          case t: CValueType[_] => Some(CArrayType(t))
          case _ => None
        }

      case _ => None
    }
  }

  // TODO Should return Option[CValue]... is this even used?
  // Yes; it is used only in RoutingTable.scala
  @inline
  final def toCValue(jval: JValue): CValue = (jval: @unchecked) match {
    case JString(s) => CString(s)
    
    case JNum(d) => {
      val ctype = forJValue(jval)
      ctype match {
        case Some(CLong) => CLong(d.toLong)
        case Some(CDouble) => CDouble(d.toDouble)
        case _ => CNum(d)
      }
    }
    
    case JBool(b)   => CBoolean(b)
    case JNull      => CNull
    case JObject(Nil) => CEmptyObject
    case JArray(Nil) => CEmptyArray
    case JArray(values) =>
      sys.error("TODO: Allow for homogeneous JArrays -> CArray.")
  }

  @inline
  final def forJValue(jval: JValue): Option[CType] = jval match {
    case JBool(_)     => Some(CBoolean)
    
    case JNum(d)      => {
      lazy val isLong = try {
        d.toLongExact
        true
      } catch {
        case _: ArithmeticException => false
      }
      
      lazy val isDouble = try {
        BigDecimal(d.toDouble.toString, MathContext.UNLIMITED) == d
      } catch {
        case _: NumberFormatException | _: ArithmeticException => false
      }
      
      if (isLong)
        Some(CLong)
      else if (isDouble)
        Some(CDouble)
      else
        Some(CNum)
    }
    
    case JString(_)   => Some(CString)
    case JNull        => Some(CNull)
    case JArray(Nil)  => Some(CEmptyArray)
    case JObject(Nil) => Some(CEmptyObject)
    case JArray(_)    => None // TODO Allow homogeneous JArrays -> CType
    case _            => None
  }

  implicit object CTypeOrder extends Order[CType] {
    def order(t1: CType, t2: CType): Ordering = (t1, t2) match {
      case (CArrayType(t1), CArrayType(t2)) => order(t1, t2)
      case (_, _) => Order[Int].order(t1.typeIndex, t2.typeIndex)
    }
  }
}

object CValueType {
  def apply[A](implicit A: CValueType[A]): CValueType[A] = A
  def apply[A](a: A)(implicit A: CValueType[A]): CWrappedValue[A] = A(a)

  // These let us do, def const[A: CValueType](a: A): CValue = CValueType[A](a)

  implicit def string: CValueType[String] = CString
  implicit def boolean: CValueType[Boolean] = CBoolean
  implicit def long: CValueType[Long] = CLong
  implicit def double: CValueType[Double] = CDouble
  implicit def bigDecimal: CValueType[BigDecimal] = CNum
  implicit def dateTime: CValueType[DateTime] = CDate
  implicit def array[A](implicit elemType: CValueType[A]) = CArrayType(elemType)
}


//
// Homogeneous arrays
// TODO Array[x] backed numeric arrays may be faster (if specialized on A).
//
case class CArray[A](value: IndexedSeq[A], cType: CArrayType[A]) extends CWrappedValue[IndexedSeq[A]]

case class CArrayType[A](elemType: CValueType[A]) extends CValueType[IndexedSeq[A]](LengthEncoded) {
  def readResolve() = CArrayType(elemType.readResolve())

  def apply(value: IndexedSeq[A]) = CArray(value, this)

  def order(as: IndexedSeq[A], bs: IndexedSeq[A]) =
    (as zip bs) map { case (a, b) =>
      elemType.order(a, b)
    } find (_ != EQ) getOrElse Ordering.fromInt(as.size - bs.size)

  def jValueFor(as: IndexedSeq[A]) =
    JArray(as.map(elemType.jValueFor _)(collection.breakOut))
}

//
// Strings
//
case class CString(value: String) extends CWrappedValue[String] {
  val cType = CString
}

case object CString extends CValueType[String](LengthEncoded) {
  def readResolve() = CString
  def order(s1: String, s2: String) = stringInstance.order(s1, s2)
  def jValueFor(s: String) = JString(s)
}

//
// Booleans
//
case class CBoolean(value: Boolean) extends CWrappedValue[Boolean] {
  val cType = CBoolean
}

case object CBoolean extends CValueType[Boolean](FixedWidth(1)) {
  def readResolve() = CBoolean
  def order(v1: Boolean, v2: Boolean) = booleanInstance.order(v1, v2)
  def jValueFor(v: Boolean) = JBool(v)
}

//
// Numerics
//
case class CLong(value: Long) extends CNumericValue[Long] {
  val cType = CLong
}

case object CLong extends CNumericType[Long](FixedWidth(8)) {
  def readResolve() = CLong
  def order(v1: Long, v2: Long) = longInstance.order(v1, v2)
  def jValueFor(v: Long): JValue = JNum(BigDecimal(v, MathContext.UNLIMITED))
  def bigDecimalFor(v: Long) = BigDecimal(v, MathContext.UNLIMITED)
}

case class CDouble(value: Double) extends CNumericValue[Double] {
  val cType = CDouble
}

case object CDouble extends CNumericType[Double](FixedWidth(8)) {
  def readResolve() = CDouble
  def order(v1: Double, v2: Double) = doubleInstance.order(v1, v2)
  def jValueFor(v: Double) = JNum(BigDecimal(v.toString, MathContext.UNLIMITED))
  def bigDecimalFor(v: Double) = BigDecimal(v, MathContext.UNLIMITED)
}

case class CNum(value: BigDecimal) extends CNumericValue[BigDecimal] {
  val cType = CNum
}

case object CNum extends CNumericType[BigDecimal](LengthEncoded) {
  def readResolve() = CNum
  def order(v1: BigDecimal, v2: BigDecimal) = bigDecimalInstance.order(v1, v2)
  def jValueFor(v: BigDecimal) = JNum(v)
  def bigDecimalFor(v: BigDecimal) = v
}

//
// Dates
//
case class CDate(value: DateTime) extends CWrappedValue[DateTime] {
  val cType = CDate
}

case object CDate extends CValueType[DateTime](FixedWidth(8)) {
  def readResolve() = CDate
  def order(v1: DateTime, v2: DateTime) = sys.error("todo")
  def jValueFor(v: DateTime) = JString(v.toString)
}

//
// Nulls
//
case object CNull extends CNullType with CNullValue {
  def readResolve() = CNull
  def toJValue = JNull
}

case object CEmptyObject extends CNullType with CNullValue {
  def readResolve() = CEmptyObject
  def toJValue = JObject(Nil)
}

case object CEmptyArray extends CNullType with CNullValue {
  def readResolve() = CEmptyArray
  def toJValue = JArray(Nil)
}

//
// Undefined - cannot ever exist as a value.
//
case object CUndefined extends CNullType with CNullValue {
  def readResolve() = CUndefined
  def toJValue = JNothing
}

