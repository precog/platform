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

sealed trait CValue extends Serializable {
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

  def toJValue: JValue
}

object CValue {
  @transient
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

  def compareValues(a: CValue, b: CValue): Int = (a,b) match {
    case (CString(as), CString(bs))   => as.compareTo(bs)
    case (CBoolean(ab), CBoolean(bb)) => ab.compareTo(bb)
    case (CLong(al), CLong(bl))       => al.compareTo(bl)
    case (CLong(al), CDouble(bd))     => al.toDouble.compareTo(bd)
    case (CLong(al), CNum(bn))        => BigDecimal(al, MathContext.UNLIMITED).compareTo(bn)
    case (CDouble(ad), CLong(bl))     => ad.compareTo(bl.toDouble)
    case (CDouble(ad), CDouble(bd))   => ad.compareTo(bd)
    case (CDouble(ad), CNum(bn))      => BigDecimal(ad, MathContext.UNLIMITED).compareTo(bn)
    case (CNum(an), CLong(bl))        => an.compareTo(BigDecimal(bl, MathContext.UNLIMITED))
    case (CNum(an), CDouble(bd))      => an.compareTo(BigDecimal(bd, MathContext.UNLIMITED))
    case (CNum(an), CNum(bn))         => an.compareTo(bn)
    case (CDate(ad), CDate(bd))       => ad.compareTo(bd)
    case (CNull, CNull)               => 0
    case (CEmptyObject, CEmptyObject) => 0
    case (CEmptyArray, CEmptyArray)   => 0
    case invalid                      => sys.error("Invalid comparison for SortingKey of " + invalid)
  }
}

sealed abstract class CType(val format: StorageFormat, val stype: SType) extends Serializable {
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

    case CUndefined   => 12
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
    case CUndefined             => sys.error("CUndefined cannot be serialized")
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
  def readResolve() = CType

  // CString
  // CBoolean
  // CLong
  // CDouble
  // CNum
  // CNull
  // CEmptyObject
  // CEmptyArray

  def of(v: CValue): CType = v match {
    case c: CString    => CString
    case c: CBoolean   => CBoolean
    case c: CLong      => CLong
    case c: CDouble    => CDouble
    case c: CNum       => CNum
    case c: CDate       => CDate
    case CNull         => CNull
    case CEmptyObject  => CEmptyObject
    case CEmptyArray   => CEmptyArray
    case CUndefined    => CUndefined
  }

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
    case _            => None
  }

  implicit object CTypeOrder extends Order[CType] {
    def order(t1: CType, t2: CType): Ordering = Order[Int].order(t1.typeIndex, t2.typeIndex)
  }
}

//
// Strings
//
case class CString(value: String) extends CValue {
  def toJValue = JString(value)
}

case object CString extends CType(LengthEncoded, SString) {
  def readResolve() = CString
  type CA = String
  val CC = classOf[String]
  def order(s1: String, s2: String) = stringInstance.order(s1, s2)
  def jvalueFor(s: String) = JString(s)
  implicit val manifest = implicitly[Manifest[String]]
}

//
// Booleans
//
case class CBoolean(value: Boolean) extends CValue {
  def toJValue = JBool(value)
}

case object CBoolean extends CType(FixedWidth(1), SBoolean) {
  def readResolve() = CBoolean
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

case class CLong(value: Long) extends CValue {
  def toJValue = JNum(BigDecimal(value, MathContext.UNLIMITED))
}
case object CLong extends CNumeric(FixedWidth(8), SDecimal) {
  def readResolve() = CLong
  type CA = Long
  val CC = classOf[Long]
  override def isNumeric: Boolean = true
  def order(v1: Long, v2: Long) = longInstance.order(v1, v2)
  def jvalueFor(v: Long) = JNum(BigDecimal(v, MathContext.UNLIMITED))
  implicit val manifest = implicitly[Manifest[Long]]
}

case class CDouble(value: Double) extends CValue {
  def toJValue = JNum(BigDecimal(value.toString, MathContext.UNLIMITED))
}
case object CDouble extends CNumeric(FixedWidth(8), SDecimal) {
  def readResolve() = CDouble
  type CA = Double
  val CC = classOf[Double]
  override def isNumeric: Boolean = true
  def order(v1: Double, v2: Double) = doubleInstance.order(v1, v2)
  def jvalueFor(v: Double) = JNum(BigDecimal(v.toString, MathContext.UNLIMITED))
  implicit val manifest = implicitly[Manifest[Double]]
}

case class CNum(value: BigDecimal) extends CValue {
  def toJValue = JNum(value)
}
case object CNum extends CNumeric(LengthEncoded, SDecimal) {
  def readResolve() = CNum
  type CA = BigDecimal
  val CC = classOf[BigDecimal]
  override def isNumeric: Boolean = true
  def order(v1: BigDecimal, v2: BigDecimal) = bigDecimalInstance.order(v1, v2)
  def jvalueFor(v: BigDecimal) = JNum(v)
  implicit val manifest = implicitly[Manifest[BigDecimal]]
}

case class CDate(value: DateTime) extends CValue {
  def toJValue = JString(value.toString)
}

case object CDate extends CType(FixedWidth(8), SString) {
  def readResolve() = CDate
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
  def readResolve() = CNull
  type CA = Null
  val CC = classOf[Null]
  def order(v1: Null, v2: Null) = EQ
  def jvalueFor(v: Null) = JNull
  def toJValue = JNull
  implicit val manifest: Manifest[Null] = implicitly[Manifest[Null]]
}

case object CUndefined extends CType(FixedWidth(0), SUndefined) with CNullType with CValue {
  def readResolve() = CUndefined
  type CA = Nothing
  val CC = classOf[Nothing]
  def order(v1: Nothing, v2: Nothing) = EQ
  def jvalueFor(v: Nothing) = JNothing
  def toJValue = JNothing
  implicit val manifest: Manifest[Nothing] = implicitly[Manifest[Nothing]]

}

case object CEmptyObject extends CType(FixedWidth(0), SObject) with CNullType with CValue {
  def readResolve() = CEmptyObject
  type CA = Null
  val CC = classOf[Null]
  def order(v1: Null, v2: Null) = EQ
  def jvalueFor(v: Null) = JObject(Nil)
  def toJValue = JObject(Nil)
  implicit val manifest: Manifest[Null] = implicitly[Manifest[Null]]
}

case object CEmptyArray extends CType(FixedWidth(0), SArray) with CNullType with CValue {
  def readResolve() = CEmptyArray
  type CA = Null
  val CC = classOf[Null]
  def order(v1: Null, v2: Null) = EQ
  def jvalueFor(v: Null) = JArray(Nil)
  def toJValue = JArray(Nil)
  implicit val manifest: Manifest[Null] = implicitly[Manifest[Null]]
}

// vim: set ts=4 sw=4 et:
