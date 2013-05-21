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
package com.precog.common

import com.precog.util._

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Versioned._

import org.joda.time.DateTime
import org.joda.time.Period

import scalaz._
import scalaz.Ordering._
import scalaz.syntax.order._
import scalaz.std._
import scalaz.std.math._
import scalaz.std.AllInstances._

import scala.{ specialized => spec }
import scala.annotation.tailrec

import _root_.java.io.{Externalizable,ObjectInput,ObjectOutput}
import _root_.java.math.MathContext

sealed trait RValue { self =>
  def toJValue: JValue

  def \(fieldName: String): RValue 

  def unsafeInsert(path: CPath, value: RValue): RValue = {
    RValue.unsafeInsert(self, path, value)
  }

  def flattenWithPath: Vector[(CPath, CValue)] = {
    def flatten0(path: CPath)(value: RValue): Vector[(CPath, CValue)] = value match {
      case RObject(fields) =>
        fields.foldLeft(Vector.empty[(CPath, CValue)]) {
          case (acc, field) =>
            acc ++ flatten0(path \ field._1)(field._2)
        }

      case RArray(elems) =>
        Vector(elems: _*).zipWithIndex.flatMap { tuple =>
          val (elem, idx) = tuple

          flatten0(path \ idx)(elem)
        }

      case (v: CValue) => Vector((path, v))
    }

    flatten0(CPath.Identity)(self)
  }
}

object RValue {
  def fromJValue(jv: JValue): RValue = jv match {
    case JObject(fields) => RObject(fields map { case (k, v) => (k, fromJValue(v)) })
    case JArray(elements) => RArray(elements map fromJValue)
    case other => CType.toCValue(other)
  }

  def unsafeInsert(rootTarget: RValue, rootPath: CPath, rootValue: RValue): RValue = {
    def rec(target: RValue, path: CPath, value: RValue): RValue = {
      if ((target == CNull || target == CUndefined) && path == CPath.Identity) value else {
        def arrayInsert(l: List[RValue], i: Int, rem: CPath, v: RValue): List[RValue] = {
          def update(l: List[RValue], j: Int): List[RValue] = l match {
            case x :: xs => (if (j == i) rec(x, rem, v) else x) :: update(xs, j + 1)
            case Nil => Nil
          }

          update(l.padTo(i + 1, CUndefined), 0)
        }

        target match {
          case obj @ RObject(fields) => path.nodes match {
            case CPathField(name) :: nodes =>
              val (child, rest) = (fields.get(name).getOrElse(CUndefined), fields - name)
              RObject(rest + (name -> rec(child, CPath(nodes), value)))

            case CPathIndex(_) :: _ => sys.error("Objects are not indexed: attempted to insert " + value + " at " + rootPath + " on " + rootTarget)
            case _ => sys.error("RValue insert would overwrite existing data: " + target + " cannot be rewritten to " + value + " at " + path +
                                " in unsafeInsert of " + rootValue + " at " + rootPath + " in " + rootTarget)
          }

          case arr @ RArray(elements) => path.nodes match {
            case CPathIndex(index) :: nodes => RArray(arrayInsert(elements, index, CPath(nodes), value))
            case CPathField(_) :: _ => sys.error("Arrays have no fields: attempted to insert " + value + " at " + rootPath + " on " + rootTarget)
            case _ => sys.error("RValue insert would overwrite existing data: " + target + " cannot be rewritten to " + value + " at " + path +
                                  " in unsafeInsert of " + rootValue + " at " + rootPath + " in " + rootTarget)
          }

          case CNull | CUndefined => path.nodes match {
            case Nil => value
            case CPathIndex(_) :: _ => rec(RArray.empty, path, value)
            case CPathField(_) :: _ => rec(RObject.empty, path, value)
            case CPathArray :: _ => sys.error("todo")
            case CPathMeta(_) :: _ => sys.error("todo")
          }

          case x => sys.error("RValue insert would overwrite existing data: " + x + " cannot be updated to " + value + " at " + path +
                              " in unsafeInsert of " + rootValue + " at " + rootPath + " in " + rootTarget)
        }
      }
    }

    rec(rootTarget, rootPath, rootValue)
  }
}

case class RObject(fields: Map[String, RValue]) extends RValue {
  def toJValue = JObject(fields map { case (k, v) => (k, v.toJValue) })
  def \(fieldName: String): RValue = fields(fieldName)
}

object RObject {
  val empty = new RObject(Map.empty)
  def apply(fields: (String, RValue)*): RValue = new RObject(Map(fields: _*))
}

case class RArray(elements: List[RValue]) extends RValue {
  def toJValue = JArray(elements map { _.toJValue })
  def \(fieldName: String): RValue = CUndefined
}

object RArray {
  val empty = new RArray(Nil)
  def apply(elements: RValue*): RValue = new RArray(elements.toList)
}

sealed trait CValue extends RValue {
  def cType: CType
  def \(fieldName: String): RValue = CUndefined
}

sealed trait CNullValue extends CValue { self: CNullType =>
  def cType: CNullType = self
}

sealed trait CWrappedValue[@spec(Boolean, Long, Double) A] extends CValue {
  def cType: CValueType[A]
  def value: A
  def toJValue = cType.jValueFor(value)
}

sealed trait CNumericValue[@spec(Long, Double) A] extends CWrappedValue[A] {
  def cType: CNumericType[A]
  def toCNum: CNum = CNum(cType.bigDecimalFor(value))
}

object CValue {
  def compareValues(a: CValue, b: CValue): Int = (a,b) match {
    case (CString(as), CString(bs)) => as.compareTo(bs)
    case (CBoolean(ab), CBoolean(bb)) => ab.compareTo(bb)
    case (CLong(al), CLong(bl)) => al.compareTo(bl)
    case (CDouble(ad), CDouble(bd)) => ad.compareTo(bd)
    case (CNum(an), CNum(bn)) => an.compare(bn)
    case (CDate(ad), CDate(bd)) => ad.compareTo(bd)
    case (CPeriod(ad), CPeriod(bd)) => (ad.toStandardDuration).compareTo(bd.toStandardDuration)
    case (CArray(as, CArrayType(atpe)), CArray(bs, CArrayType(btpe))) if atpe == btpe =>
      (as.view zip bs.view) map { case (a, b) =>
        compareValues(atpe(a), btpe(b))
      } find (_ != 0) getOrElse (as.size - bs.size)
    case (a: CNumericValue[_], b: CNumericValue[_]) =>
      compareValues(a.toCNum, b.toCNum) // The only safe way to compare any mix of the 3 types.
    case (a: CNullValue, b: CNullValue) if a.cType == b.cType => 0
    case (a, b) => a.cType.typeIndex - b.cType.typeIndex
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
  def isNumeric: Boolean = false

  @inline 
  private[common] final def typeIndex = this match {
    case CUndefined    => 0

    case CBoolean      => 1

    case CString       => 2
    
    case CLong         => 4
    case CDouble       => 6
    case CNum          => 7
    
    case CEmptyObject  => 8

    case CEmptyArray   => 9

    case CArrayType(_) => 10 // TODO: Should this account for the element type?

    case CNull         => 11

    case CDate         => 12
    case CPeriod       => 13
  }
}

sealed trait CNullType extends CType with CNullValue

sealed trait CValueType[@spec(Boolean, Long, Double) A] extends CType { self =>
  def manifest: Manifest[A]

  def readResolve(): CValueType[A]
  def apply(a: A): CWrappedValue[A]
  def order(a: A, b: A): Ordering
  def jValueFor(a: A): JValue
}

sealed trait CNumericType[@spec(Long, Double) A] extends CValueType[A] {
  override def isNumeric: Boolean = true
  def bigDecimalFor(a: A): BigDecimal
}

object CType {
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
    case CPeriod                => "Period"
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
    case "Period"     => Some(CPeriod)
    case _ => None
  }
    
  implicit val decomposer : Decomposer[CType] = new Decomposer[CType] {
    def decompose(ctype : CType) : JValue = JString(nameOf(ctype))
  }

  implicit val extractor : Extractor[CType] = new Extractor[CType] {
    def validated(obj : JValue) : Validation[Extractor.Error,CType] = 
      obj.validated[String].map( fromName _ ) match {
        case Success(Some(t)) => Success(t)
        case Success(None)    => Failure(Extractor.Invalid("Unknown type."))
        case Failure(f)       => Failure(f)
      }
  }

  def readResolve() = CType

  def of(v: CValue): CType = v.cType

  def canCompare(t1: CType, t2: CType): Boolean =
    (t1 == t2) || (t1.isNumeric && t2.isNumeric)

  def unify(t1: CType, t2: CType): Option[CType] = {
    (t1, t2) match {
      case (CLong, CLong)     => Some(CLong)
      case (CLong, CDouble)   => Some(CNum)
      case (CLong, CNum)      => Some(CNum)
      case (CDouble, CLong)   => Some(CNum)
      case (CDouble, CDouble) => Some(CDouble)
      case (CDouble, CNum)    => Some(CNum)
      case (CNum, CLong)      => Some(CNum)
      case (CNum, CDouble)    => Some(CNum)
      case (CNum, CNum)       => Some(CNum)

      case (CString, CString) => Some(CString)

      case (CDate, CDate) => Some(CDate)
      case (CPeriod, CPeriod) => Some(CPeriod)

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
  private val emptyJArray = JArray(Nil)
  private val emptyJObject = JObject(Map())
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
    case JObject(es) if es.size == 0 => CEmptyObject
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
    case o: JObject if o == emptyJObject => Some(CEmptyObject)
    case o: JArray if o == emptyJArray => None // TODO Allow homogeneous JArrays -> CType
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
  def apply[@spec(Boolean, Long, Double) A](implicit A: CValueType[A]): CValueType[A] = A
  def apply[@spec(Boolean, Long, Double) A](a: A)(implicit A: CValueType[A]): CWrappedValue[A] = A(a)

  // These let us do, def const[A: CValueType](a: A): CValue = CValueType[A](a)

  implicit def string: CValueType[String] = CString
  implicit def boolean: CValueType[Boolean] = CBoolean
  implicit def long: CValueType[Long] = CLong
  implicit def double: CValueType[Double] = CDouble
  implicit def bigDecimal: CValueType[BigDecimal] = CNum
  implicit def dateTime: CValueType[DateTime] = CDate
  implicit def period: CValueType[Period] = CPeriod
  implicit def array[@spec(Boolean, Long, Double) A](implicit elemType: CValueType[A]) = CArrayType(elemType)
}


//
// Homogeneous arrays
//
case class CArray[@spec(Boolean, Long, Double) A](value: Array[A], cType: CArrayType[A]) extends CWrappedValue[Array[A]] {
  private final def leafEquiv[@spec(Boolean, Long, Double) A](as: Array[A], bs: Array[A]): Boolean = {
    var i = 0
    var result = as.length == bs.length
    while (result && i < as.length) {
      result = as(i) == bs(i)
      i += 1
    }
    result
  }

  private final def equiv(a: Any, b: Any, elemType: CValueType[_]): Boolean = elemType match {
    case CBoolean =>
      leafEquiv(a.asInstanceOf[Array[Boolean]], b.asInstanceOf[Array[Boolean]])

    case CLong =>
      leafEquiv(a.asInstanceOf[Array[Long]], b.asInstanceOf[Array[Long]])

    case CDouble =>
      leafEquiv(a.asInstanceOf[Array[Double]], b.asInstanceOf[Array[Double]])

    case CArrayType(elemType) =>
      val as = a.asInstanceOf[Array[Array[_]]]
      val bs = b.asInstanceOf[Array[Array[_]]]
      var i = 0
      var result = as.length == bs.length
      while (result && i < as.length) {
        result = equiv(as(i), bs(i), elemType)
        i += 1
      }
      result

    case _ =>
      leafEquiv(a.asInstanceOf[Array[AnyRef]], b.asInstanceOf[Array[AnyRef]])
  }

  override def equals(that: Any): Boolean = that match {
    case v @ CArray(_, thatCType) if cType == thatCType =>
      equiv(value, v.value, cType.elemType)

    case _ => false
  }

  override def toString: String = value.mkString("CArray(Array(", ", ", "), " + cType.toString + ")")
}

case object CArray {
  def apply[@spec(Boolean, Long, Double) A](as: Array[A])(implicit elemType: CValueType[A]): CArray[A] =
    CArray(as, CArrayType(elemType))
}

case class CArrayType[@spec(Boolean, Long, Double) A](elemType: CValueType[A]) extends CValueType[Array[A]] {
  // Spec. bug: Leave lazy here.
  lazy val manifest: Manifest[Array[A]] = elemType.manifest.arrayManifest

  type tpe = A

  def readResolve() = CArrayType(elemType.readResolve())

  def apply(value: Array[A]) = CArray(value, this)

  def order(as: Array[A], bs: Array[A]) =
    (as zip bs) map { case (a, b) =>
      elemType.order(a, b)
    } find (_ != EQ) getOrElse Ordering.fromInt(as.size - bs.size)

  def jValueFor(as: Array[A]) =
    sys.error("HOMOGENEOUS ARRAY ESCAPING! ALERT! ALERT!")
}

//
// Strings
//
case class CString(value: String) extends CWrappedValue[String] {
  val cType = CString
}

case object CString extends CValueType[String] {
  val manifest: Manifest[String] = implicitly[Manifest[String]]
  def readResolve() = CString
  def order(s1: String, s2: String) = stringInstance.order(s1, s2)
  def jValueFor(s: String) = JString(s)
}

//
// Booleans
//
sealed abstract class CBoolean(val value: Boolean) extends CWrappedValue[Boolean] {
  val cType = CBoolean
}

case object CTrue extends CBoolean(true)
case object CFalse extends CBoolean(false)

object CBoolean extends CValueType[Boolean] {
  def apply(value: Boolean) = if (value) CTrue else CFalse
  def unapply(cbool: CBoolean) = Some(cbool.value)
  val manifest: Manifest[Boolean] = implicitly[Manifest[Boolean]]
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

case object CLong extends CNumericType[Long] {
  val manifest: Manifest[Long] = implicitly[Manifest[Long]]
  def readResolve() = CLong
  def order(v1: Long, v2: Long) = longInstance.order(v1, v2)
  def jValueFor(v: Long): JValue = JNum(BigDecimal(v, MathContext.UNLIMITED))
  def bigDecimalFor(v: Long) = BigDecimal(v, MathContext.UNLIMITED)
}

case class CDouble(value: Double) extends CNumericValue[Double] {
  val cType = CDouble
}

case object CDouble extends CNumericType[Double] {
  val manifest: Manifest[Double] = implicitly[Manifest[Double]]
  def readResolve() = CDouble
  def order(v1: Double, v2: Double) = doubleInstance.order(v1, v2)
  def jValueFor(v: Double) = JNum(BigDecimal(v.toString, MathContext.UNLIMITED))
  def bigDecimalFor(v: Double) = BigDecimal(v, MathContext.UNLIMITED)
}

case class CNum(value: BigDecimal) extends CNumericValue[BigDecimal] {
  val cType = CNum
}

case object CNum extends CNumericType[BigDecimal] {
  val manifest: Manifest[BigDecimal] = implicitly[Manifest[BigDecimal]]
  def readResolve() = CNum
  def order(v1: BigDecimal, v2: BigDecimal) = bigDecimalInstance.order(v1, v2)
  def jValueFor(v: BigDecimal) = JNum(v)
  def bigDecimalFor(v: BigDecimal) = v
}

//
// Dates and Periods
//
case class CDate(value: DateTime) extends CWrappedValue[DateTime] {
  val cType = CDate
}

case object CDate extends CValueType[DateTime] {
  val manifest: Manifest[DateTime] = implicitly[Manifest[DateTime]]
  def readResolve() = CDate
  def order(v1: DateTime, v2: DateTime) = sys.error("todo")
  def jValueFor(v: DateTime) = JString(v.toString)
}

case class CPeriod(value: Period) extends CWrappedValue[Period] {
  val cType = CPeriod
}

case object CPeriod extends CValueType[Period] {
  val manifest: Manifest[Period] = implicitly[Manifest[Period]]
  def readResolve() = CPeriod
  def order(v1: Period, v2: Period) = sys.error("todo")
  def jValueFor(v: Period) = JString(v.toString)
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
// Undefined
//
case object CUndefined extends CNullType with CNullValue {
  def readResolve() = CUndefined
  def toJValue = JUndefined
}
