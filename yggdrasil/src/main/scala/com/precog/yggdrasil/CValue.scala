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

import com.precog.util._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import scalaz._
import scalaz.syntax.order._
import scalaz.std.AllInstances._

sealed abstract class CValue {
  @inline private[CValue] final def typeIndex: Int = this match {
    case CString(v) => 0
    case CBoolean(v) => 1
    case CInt(v) => 2
    case CLong(v) => 3
    case CDouble(v) => 4
    case CNum(v) => 5
    case CEmptyObject => 6
    case CEmptyArray => 7
    case CNull => 8
  }

  @inline final def toSValue: SValue = this match {
    case CString(v) => SString(v)
    case CBoolean(v) => if (v) STrue else SFalse
    case CInt(v) => SDecimal(v)
    case CLong(v) => SDecimal(v)
    case CDouble(v) => SDecimal(v)
    case CNum(v) => SDecimal(v)
    case CEmptyObject => SObject(Map())
    case CEmptyArray => SArray(Vector())
    case CNull => SNull
  }
}

object CValue {
  implicit object order extends Order[CValue] {
    def order(v1: CValue, v2: CValue) = (v1, v2) match {
      case (CString(a), CString(b)) => Order[String].order(a, b)
      case (CBoolean(a), CBoolean(b)) => Order[Boolean].order(a, b)
      case (CInt(a), CInt(b)) => Order[Int].order(a, b)
      case (CLong(a), CLong(b)) => Order[Long].order(a, b)
      case (CFloat(a), CFloat(b)) => Order[Float].order(a, b)
      case (CDouble(a), CDouble(b)) => Order[Double].order(a, b)
      case (CNum(a), CNum(b)) => Order[BigDecimal].order(a, b)
      case (vx, vy) => Order[Int].order(vx.typeIndex, vy.typeIndex)
    }
  }
}

sealed trait CType {
  def format: StorageFormat

  def stype: SType

  @inline private[CType] final def typeIndex = this match {
    case CBoolean => 0

    case CStringFixed(_) => 1
    case CStringArbitrary => 2
    
    case CInt => 3
    case CLong => 4
    case CFloat => 5
    case CDouble => 6
    case CDecimalArbitrary => 7
    
    case CEmptyObject => 8
    case CEmptyArray => 9
    case CNull => 10
  }
  
  def =~(tpe: SType): Boolean = (this, tpe) match {
    case (CBoolean, SBoolean) => true  

    case (CStringFixed(_), SString) => true
    case (CStringArbitrary, SString) => true
    
    case (CInt, SDecimal) => true
    case (CLong, SDecimal) => true
    case (CFloat, SDecimal) => true
    case (CDouble, SDecimal) => true
    case (CDecimalArbitrary, SDecimal) => true
    
    case (CEmptyObject, SObject) => true
    case (CEmptyArray, SArray) => true
    case (CNull, SNull) => true

    case _ => false
  }
}

trait CTypeSerialization {
  def nameOf(c: CType): String = c match {
    case CStringFixed(width)    => "String("+width+")"
    case CStringArbitrary       => "String"
    case CBoolean               => "Boolean"
    case CInt                   => "Int"
    case CLong                  => "Long"
    case CFloat                 => "Float"
    case CDouble                => "Double"
    case CDecimalArbitrary      => "Decimal"
    case CNull                  => "Null"
    case CEmptyObject           => "EmptyObject"
    case CEmptyArray            => "EmptyArray"
  } 

  def fromName(n: String): Option[CType] = {
    val FixedStringR = """String\(\d+\)""".r
    n match {
      case FixedStringR(w) => Some(CStringFixed(w.toInt))
      case "String"        => Some(CStringArbitrary)
      case "Boolean"       => Some(CBoolean)
      case "Int"           => Some(CInt)
      case "Long"          => Some(CLong)
      case "Float"         => Some(CFloat)
      case "Double"        => Some(CDouble)
      case "Decimal"       => Some(CDecimalArbitrary)
      case "Null"          => Some(CNull)
      case "EmptyObject"   => Some(CEmptyObject)
      case "EmptyArray"    => Some(CEmptyArray)
      case _ => None
    }
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
  // Note this conversion has a peer for SValues that should always be changed
  // in conjunction with this mapping.
  @inline
  final def toCValue(jval: JValue): CValue = jval match {
    case JString(s) => CString(s)
    case JInt(i) => sizedIntCValue(i)
    case JDouble(d) => CDouble(d)
    case JBool(b) => CBoolean(b)
    case JNull => CNull
    case JArray(Nil) => CEmptyArray
    case JObject(Nil) => CEmptyObject
    case _ => sys.error("unpossible: " + jval.getClass.getName)
  }

  @inline
  final def forValue(jval: JValue): Option[CType] = jval match {
    case JBool(_)     => Some(CBoolean)
    case JInt(bi)     => Some(sizedIntCType(bi))
    case JDouble(_)   => Some(CDouble)
    case JString(_)   => Some(CStringArbitrary)
    case JNull        => Some(CNull)
    case JArray(Nil)  => Some(CEmptyArray)
    case JObject(Nil) => Some(CEmptyObject)
    case _            => None
  }

  @inline
  private final def sizedIntCValue(bi: BigInt): CValue = {
    if(bi.isValidInt) {
      CInt(bi.intValue)
    } else if(isValidLong(bi)) {
      CLong(bi.longValue)
    } else {
      CNum(BigDecimal(bi))
    }
  }

  @inline
  private final def sizedIntCType(bi: BigInt): CType = {
   if(bi.isValidInt) {
      CInt 
    } else if(isValidLong(bi)) {
      CLong
    } else {
      CDecimalArbitrary
    }   
  }

  implicit object CTypeOrder extends Order[CType] {
    def order(t1: CType, t2: CType): Ordering = Order[Int].order(t1.typeIndex, t2.typeIndex)
  }
}

// vim: set ts=4 sw=4 et:
//
// Strings
//
case class CString(value: String) extends CValue 

case class CStringFixed(width: Int) extends CType {
  def format = FixedWidth(width)  
  val stype = SString
}

case object CStringArbitrary extends CType {
  val format = LengthEncoded  
  val stype = SString
}

//
// Booleans
//
case class CBoolean(value: Boolean) extends CValue 

case object CBoolean extends CType {
  val format = FixedWidth(1)
  val stype = SBoolean
}

//
// Numerics
//
case class CInt(value: Int) extends CValue 

case object CInt extends CType {
  val format = FixedWidth(4)
  val stype = SDecimal
}

case class CLong(value: Long) extends CValue 

case object CLong extends CType {
  val format = FixedWidth(8)
  val stype = SDecimal
}

case class CFloat(value: Float) extends CValue 

case object CFloat extends CType {
  val format = FixedWidth(4)
  val stype = SDecimal
}

case class CDouble(value: Double) extends CValue 

case object CDouble extends CType {
  val format = FixedWidth(8)
  val stype = SDecimal
}

case class CNum(value: BigDecimal) extends CValue 

case object CDecimalArbitrary extends CType {
  val format = LengthEncoded  
  val stype = SDecimal
}

//
// Nulls
//
case object CEmptyObject extends CValue with CType {
  val format = FixedWidth(0)
  val stype = SObject
}

case object CEmptyArray extends CValue with CType {
  val format = FixedWidth(0)
  val stype = SArray
}

case object CNull extends CValue with CType {
  val format = FixedWidth(0)
  val stype = SNull
}

