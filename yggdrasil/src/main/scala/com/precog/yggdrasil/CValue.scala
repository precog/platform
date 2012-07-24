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

sealed trait CValue {
  @inline private[CValue] final def typeIndex: Int = (this : @unchecked) match {
    case CString(v)  => 0
    case CBoolean(v) => 1
    case CLong(v)    => 3
    case CDouble(v)  => 5
    case CNum(v)     => 6
  }

  @inline final def toSValue: SValue = (this : @unchecked) match {
    case CString(v)  => SString(v)
    case CBoolean(v) => if (v) STrue else SFalse
    case CLong(v)    => SDecimal(v)
    case CDouble(v)  => SDecimal(v)
    case CNum(v)     => SDecimal(v)
  }
}

object CValue {
  implicit object order extends Order[CValue] {
    def order(v1: CValue, v2: CValue) = (v1, v2) match {
      case (CString(a), CString(b)) => Order[String].order(a, b)
      case (CBoolean(a), CBoolean(b)) => Order[Boolean].order(a, b)
      case (CLong(a), CLong(b)) => Order[Long].order(a, b)
      case (CDouble(a), CDouble(b)) => Order[Double].order(a, b)
      case (CNum(a), CNum(b)) => Order[BigDecimal].order(a, b)
      case (vx, vy) => Order[Int].order(vx.typeIndex, vy.typeIndex)
    }
  }
}

sealed abstract class CType(val format: StorageFormat, val stype: SType) {
  type CA

  val CC: Class[CA]

  implicit val manifest: Manifest[CA]

  def isNumeric: Boolean = false

  def order(a: CA, b: CA): Ordering

  def jvalueFor(a: CA): JValue

  @inline 
  private[CType] final def typeIndex = this match {
    case CBoolean     => 0

    case CString      => 2
    
    case CLong        => 4
    case CDouble      => 6
    case CNum         => 7
    
    case CEmptyObject => 8
    case CEmptyArray  => 9
    case CNull        => 10

    case CDate        => 11
  }
  
  def =~(tpe: SType): Boolean = (this, tpe) match {
    case (CBoolean, SBoolean)    => true  

    case (CString, SString)      => true
    
    case (CLong, SDecimal)       => true
    case (CDouble, SDecimal)     => true
    case (CNum, SDecimal)        => true
    
    case (CEmptyObject, SObject) => true
    case (CEmptyArray, SArray)   => true
    case (CNull, SNull)          => true

    case _ => false
  }
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
    case CDate                  => "Timestamp"
  } 

  def fromName(n: String): Option[CType] = n match {
    case "String"        => Some(CString)
    case "Boolean"       => Some(CBoolean)
    case "Long"          => Some(CLong)
    case "Double"        => Some(CDouble)
    case "Decimal"       => Some(CNum)
    case "Null"          => Some(CNull)
    case "EmptyObject"   => Some(CEmptyObject)
    case "EmptyArray"    => Some(CEmptyArray)
    case "Timestamp"     => Some(CDate)
    case _ => None
  }
    
  implicit val PrimtitiveTypeDecomposer : Decomposer[CType] = new Decomposer[CType] {
    def decompose(ctype : CType) : JValue = JString(nameOf(ctype))
  }

  implicit val STypeExtractor : Extractor[CType] = new Extractor[CType] with ValidatedExtraction[CType] {
    override def validated(obj : JValue) : Validation[Extractor.Error,CType] = 
      obj.validated[String].map( fromName _ ) match {
        case Success(Some(t)) => Success(t)
        case Success(None)    => Failure(Extractor.Invalid("Unknown type."))
        case Failure(f)       => Failure(f)
      }
  }
}

object CType extends CTypeSerialization {

  // CString
  // CBoolean
  // CLong
  // CDouble
  // CNum
  // CNull
  // CEmptyObject
  // CEmptyArray

  def canCompare(t1: CType, t2: CType): Boolean = (t1, t2) match {
    case (n1: CNumeric, n2: CNumeric) => true
    case (a, b) if a.getClass() == b.getClass() => true
    case _ => false
  }

  def unify(t1: CType, t2: CType): Option[CType] = {
    (t1, t2) match {
      case (CLong, CLong)     => Some(CLong)
      case (CLong, CDouble)   => Some(CDouble)
      case (CLong, CNum)      => Some(CNum)
      case (CDouble, CLong)   => Some(CDouble)
      case (CDouble, CDouble) => Some(CDouble)
      case (CDouble, CNum)    => Some(CNum)
      case (CNum, CLong)      => Some(CNum)
      case (CNum, CDouble)    => Some(CNum)
      case (CNum, CNum)       => Some(CNum)

      case (CString, CString) => Some(CString)

      case _ => None
    }
  }
  @inline
  final def toCValue(jval: JValue): CValue = jval match {
    case JString(s) => CString(s)
    case JInt(i)    => sizedIntCValue(i)
    case JDouble(d) => CDouble(d)
    case JBool(b)   => CBoolean(b)
    case _          => null
  }

  @inline
  final def forJValue(jval: JValue): Option[CType] = jval match {
    case JBool(_)     => Some(CBoolean)
    case JInt(bi)     => Some(sizedIntCType(bi))
    case JDouble(_)   => Some(CDouble)
    case JString(_)   => Some(CString)
    case JNull        => Some(CNull)
    case JArray(Nil)  => Some(CEmptyArray)
    case JObject(Nil) => Some(CEmptyObject)
    case _            => None
  }

  @inline
  final def sizedIntCValue(bi: BigInt): CValue = {
    if(bi.isValidInt || isValidLong(bi)) {
      CLong(bi.longValue)
    } else {
      CNum(BigDecimal(bi))
    }
  }

  @inline
  private final def sizedIntCType(bi: BigInt): CType = {
   if(bi.isValidInt || isValidLong(bi)) {
      CLong
    } else {
      CNum
    }   
  }

  implicit object CTypeOrder extends Order[CType] {
    def order(t1: CType, t2: CType): Ordering = Order[Int].order(t1.typeIndex, t2.typeIndex)
  }
}

//
// Strings
//
case class CString(value: String) extends CValue 

case object CString extends CType(LengthEncoded, SString) {
  type CA = String
  val CC = classOf[String]
  def order(s1: String, s2: String) = stringInstance.order(s1, s2)
  def jvalueFor(s: String) = JString(s)
  implicit val manifest = implicitly[Manifest[String]]
}

//
// Booleans
//
case class CBoolean(value: Boolean) extends CValue 
case object CBoolean extends CType(FixedWidth(1), SBoolean) {
  type CA = Boolean
  val CC = classOf[Boolean]
  def order(v1: Boolean, v2: Boolean) = booleanInstance.order(v1, v2)
  def jvalueFor(v: Boolean) = JBool(v)
  implicit val manifest = implicitly[Manifest[Boolean]]
}

//
// Numerics
//
sealed abstract class CNumeric(format: StorageFormat, stype: SType) extends CType(format, stype)

case class CLong(value: Long) extends CValue 
case object CLong extends CNumeric(FixedWidth(8), SDecimal) {
  type CA = Long
  val CC = classOf[Long]
  override def isNumeric: Boolean = true
  def order(v1: Long, v2: Long) = longInstance.order(v1, v2)
  def jvalueFor(v: Long) = JInt(v)
  implicit val manifest = implicitly[Manifest[Long]]
}

case class CDouble(value: Double) extends CValue 
case object CDouble extends CNumeric(FixedWidth(8), SDecimal) {
  type CA = Double
  val CC = classOf[Double]
  override def isNumeric: Boolean = true
  def order(v1: Double, v2: Double) = doubleInstance.order(v1, v2)
  def jvalueFor(v: Double) = JDouble(v)
  implicit val manifest = implicitly[Manifest[Double]]
}

case class CNum(value: BigDecimal) extends CValue 
case object CNum extends CNumeric(LengthEncoded, SDecimal) {
  type CA = BigDecimal
  val CC = classOf[BigDecimal]
  override def isNumeric: Boolean = true
  def order(v1: BigDecimal, v2: BigDecimal) = bigDecimalInstance.order(v1, v2)
  def jvalueFor(v: BigDecimal) = JDouble(v.toDouble)
  implicit val manifest = implicitly[Manifest[BigDecimal]]
}

case class CDate(value: DateTime) extends CValue
case object CDate extends CType(FixedWidth(8), SString) {
  type CA = DateTime
  val CC = classOf[DateTime]
  def order(v1: DateTime, v2: DateTime) = sys.error("todo")
  def jvalueFor(v: DateTime) = JString(v.toString)
  implicit val manifest = implicitly[Manifest[DateTime]]
}

sealed trait CNullType extends CType

//
// Nulls
//
case object CNull extends CType(FixedWidth(0), SNull) with CNullType with CValue {
  type CA = Null
  val CC = classOf[Null]
  def order(v1: Null, v2: Null) = EQ
  def jvalueFor(v: Null) = JNull
  implicit val manifest: Manifest[Null] = implicitly[Manifest[Null]]
}

case object CEmptyObject extends CType(FixedWidth(0), SObject) with CNullType with CValue {
  type CA = Null
  val CC = classOf[Null]
  def order(v1: Null, v2: Null) = EQ
  def jvalueFor(v: Null) = JObject(Nil)
  implicit val manifest: Manifest[Null] = implicitly[Manifest[Null]]
}

case object CEmptyArray extends CType(FixedWidth(0), SArray) with CNullType with CValue {
  type CA = Null
  val CC = classOf[Null]
  def order(v1: Null, v2: Null) = EQ
  def jvalueFor(v: Null) = JArray(Nil)
  implicit val manifest: Manifest[Null] = implicitly[Manifest[Null]]
}

// vim: set ts=4 sw=4 et:
