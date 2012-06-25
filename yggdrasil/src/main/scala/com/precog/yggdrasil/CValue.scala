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

sealed abstract class CValue {
  @inline 
  private[CValue] final def typeIndex: Int = this match {
    case CString(v) => 0
    case CBoolean(v) => 1
    case CInt(v) => 2
    case CLong(v) => 3
    case CFloat(v) => 4
    case CDouble(v) => 5
    case CNum(v) => 6
  }

  @inline 
  final def toSValue: SValue = this match {
    case CString(v) => SString(v)
    case CBoolean(v) => if (v) STrue else SFalse
    case CInt(v) => SDecimal(v)
    case CLong(v) => SDecimal(v)
    case CFloat(v) => SDecimal(v)
    case CDouble(v) => SDecimal(v)
    case CNum(v) => SDecimal(v)
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

sealed abstract class CType(val format: StorageFormat, val stype: SType) {
  type CA

  val CC: Class[CA]

  implicit val manifest: Manifest[CA]

  def order(a: CA, b: CA): Ordering

  def jvalueFor(a: CA): JValue

  @inline 
  private[CType] final def typeIndex = this match {
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

  // CStringFixed(width)
  // CStringArbitrary
  // CBoolean
  // CInt
  // CLong
  // CFloat
  // CDouble
  // CDecimalArbitrary
  // CNull
  // CEmptyObject
  // CEmptyArray

  def unify(t1: CType, t2: CType): Option[CType] = {
    (t1, t2) match {
      case (CInt, CInt) => Some(CInt )
      case (CInt, CLong) => Some(CLong)
      case (CInt, CFloat) => Some(CFloat)
      case (CInt, CDouble) => Some(CDouble)
      case (CInt, CDecimalArbitrary) => Some(CDecimalArbitrary)
      case (CLong, CInt) => Some(CLong)
      case (CLong, CLong) => Some(CLong)
      case (CLong, CFloat) => Some(CFloat)
      case (CLong, CDouble) => Some(CDouble)
      case (CLong, CDecimalArbitrary) => Some(CDecimalArbitrary)
      case (CFloat, CInt) => Some(CFloat)
      case (CFloat, CLong) => Some(CFloat)
      case (CFloat, CFloat) => Some(CFloat)
      case (CFloat, CDouble) => Some(CDouble)
      case (CFloat, CDecimalArbitrary) => Some(CDecimalArbitrary)
      case (CDouble, CInt) => Some(CDouble)
      case (CDouble, CLong) => Some(CDouble)
      case (CDouble, CFloat) => Some(CDouble)
      case (CDouble, CDouble) => Some(CDouble)
      case (CDouble, CDecimalArbitrary) => Some(CDecimalArbitrary)
      case (CDecimalArbitrary, CInt) => Some(CDecimalArbitrary)
      case (CDecimalArbitrary, CLong) => Some(CDecimalArbitrary)
      case (CDecimalArbitrary, CFloat) => Some(CDecimalArbitrary)
      case (CDecimalArbitrary, CDouble) => Some(CDecimalArbitrary)
      case (CDecimalArbitrary, CDecimalArbitrary) => Some(CDecimalArbitrary)

      case (f1 @ CStringFixed(w1), f2 @ CStringFixed(w2)) => Some(if (w1 > w2) f1 else f2)
      case (CStringFixed(_), CStringArbitrary) => Some(CStringArbitrary)
      case (CStringArbitrary, CStringArbitrary) => Some(CStringArbitrary)
      case (CStringArbitrary, CStringFixed(_)) => Some(CStringArbitrary)

      case _ => None
    }
  }
  @inline
  final def toCValue(jval: JValue): CValue = jval match {
    case JString(s) => CString(s)
    case JInt(i) => sizedIntCValue(i)
    case JDouble(d) => CDouble(d)
    case JBool(b) => CBoolean(b)
    case _ => null
  }

  @inline
  final def forJValue(jval: JValue): Option[CType] = jval match {
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
  final def sizedIntCValue(bi: BigInt): CValue = {
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

case class CStringFixed(width: Int) extends CType(FixedWidth(width), SString) {
  type CA = String
  val CC = classOf[String]
  def order(s1: String, s2: String) = stringInstance.order(s1, s2)
  def jvalueFor(s: String) = JString(s)
  implicit val manifest = implicitly[Manifest[String]]
}

case object CStringArbitrary extends CType(LengthEncoded, SString) {
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
case class CInt(value: Int) extends CValue 
case object CInt extends CType(FixedWidth(4), SDecimal) {
  type CA = Int
  val CC = classOf[Int]
  def order(v1: Int, v2: Int) = intInstance.order(v1, v2)
  def jvalueFor(v: Int) = JInt(v)
  implicit val manifest = implicitly[Manifest[Int]]
}

case class CLong(value: Long) extends CValue 
case object CLong extends CType(FixedWidth(8), SDecimal) {
  type CA = Long
  val CC = classOf[Long]
  def order(v1: Long, v2: Long) = longInstance.order(v1, v2)
  def jvalueFor(v: Long) = JInt(v)
  implicit val manifest = implicitly[Manifest[Long]]
}

case class CFloat(value: Float) extends CValue 
case object CFloat extends CType(FixedWidth(4), SDecimal) {
  type CA = Float
  val CC = classOf[Float]
  def order(v1: Float, v2: Float) = floatInstance.order(v1, v2)
  def jvalueFor(v: Float) = JDouble(v)
  implicit val manifest = implicitly[Manifest[Float]]
}

case class CDouble(value: Double) extends CValue 
case object CDouble extends CType(FixedWidth(8), SDecimal) {
  type CA = Double
  val CC = classOf[Double]
  def order(v1: Double, v2: Double) = doubleInstance.order(v1, v2)
  def jvalueFor(v: Double) = JDouble(v)
  implicit val manifest = implicitly[Manifest[Double]]
}

case class CNum(value: BigDecimal) extends CValue 
case object CDecimalArbitrary extends CType(LengthEncoded, SDecimal) {
  type CA = BigDecimal
  val CC = classOf[BigDecimal]
  def order(v1: BigDecimal, v2: BigDecimal) = bigDecimalInstance.order(v1, v2)
  def jvalueFor(v: BigDecimal) = JDouble(v.toDouble)
  implicit val manifest = implicitly[Manifest[BigDecimal]]
}

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
case object CNull extends CType(FixedWidth(0), SNull) with CNullType {
  type CA = Null
  val CC = classOf[Null]
  def order(v1: Null, v2: Null) = EQ
  def jvalueFor(v: Null) = JNull
  implicit val manifest: Manifest[Null] = implicitly[Manifest[Null]]
}

case object CEmptyObject extends CType(FixedWidth(0), SObject) with CNullType {
  type CA = Null
  val CC = classOf[Null]
  def order(v1: Null, v2: Null) = EQ
  def jvalueFor(v: Null) = JObject(Nil)
  implicit val manifest: Manifest[Null] = implicitly[Manifest[Null]]
}

case object CEmptyArray extends CType(FixedWidth(0), SArray) with CNullType {
  type CA = Null
  val CC = classOf[Null]
  def order(v1: Null, v2: Null) = EQ
  def jvalueFor(v: Null) = JArray(Nil)
  implicit val manifest: Manifest[Null] = implicitly[Manifest[Null]]
}
