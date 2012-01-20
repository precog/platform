package com.reportgrid.yggdrasil

import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import scalaz._

trait SValue {
  def fold[A](
    obj:    Map[String, SValue] => A,
    arr:    Vector[SValue] => A,
    str:    String => A,
    bool:   Boolean => A,
    long:   Long => A,
    double: Double => A,
    num:    BigDecimal => A,
    nul:    => A
  ): A

  def mapObjectOr[A](a: => A)(f: Map[String, SValue] => A): A = {
    val d = (_: Any) => a
    fold(f, d, d, d, d, d, d, a)
  }

  def mapArrayOr[A](a: => A)(f: Vector[SValue] => A): A = {
    val d = (_: Any) => a
    fold(d, f, d, d, d, d, d, a)
  }

  def mapStringOr[A](a: => A)(f: String => A): A = {
    val d = (_: Any) => a
    fold(d, d, f, d, d, d, d, a)
  }

  def mapBooleanOr[A](a: => A)(f: Boolean => A): A = {
    val d = (_: Any) => a
    fold(d, d, d, f, d, d, d, a)
  }

  def mapLongOr[A](a: => A)(f: Long => A): A = {
    val d = (_: Any) => a
    fold(d, d, d, d, f, d, d, a)
  }

  def mapDoubleOr[A](a: => A)(f: Double => A): A = {
    val d = (_: Any) => a
    fold(d, d, d, d, d, f, d, a)
  }

  def mapBigDecimalOr[A](a: => A)(f: BigDecimal => A): A = {
    val d = (_: Any) => a
    fold(d, d, d, d, d, d, f, a)
  }

  def mapNullOr[A](a: => A): A = {
    val d = (_: Any) => a
    fold(d, d, d, d, d, d, d, a)
  }
}

sealed trait StorageFormat 

case object CompositeFormat extends StorageFormat 
case object LengthEncoded extends StorageFormat 
case class  FixedWidth(width: Int) extends StorageFormat 

sealed trait SType { 
  def format: StorageFormat 
}

trait STypeSerialization {
  def nameOf(c: SType): String = c match {
    case SObject                => "Object"
    case SEmptyObject           => "SEmptyObject"
    case SArray                 => "Array"
    case SEmptyArray            => "EmptyArray"
    case SStringFixed(width)    => "String("+width+")"
    case SStringArbitrary       => "String"
    case SBoolean               => "Boolean"
    case SLong                  => "Long"
    case SDouble                => "Double"
    case SDecimalArbitrary      => "Decimal"
    case SNull                  => "Null"
  } 

  def fromName(n: String): Option[SType] = {
    val FixedStringR = """String\(\d+\)""".r
    n match {
      case FixedStringR(w)      => Some(SStringFixed(w.toInt))
      case "Object"      => Some(SObject)
      case "EmptyObject" => Some(SEmptyObject)
      case "Array"       => Some(SArray)
      case "EmptyArray"  => Some(SEmptyArray)
      case "String"      => Some(SStringArbitrary)
      case "Boolean"     => Some(SBoolean)
      case "Long"        => Some(SLong)
      case "Double"      => Some(SDouble)
      case "Decimal"     => Some(SDecimalArbitrary)
      case "Null"        => Some(SNull)
      case _ => None
    }
  }
    
  implicit val PrimtitiveTypeDecomposer : Decomposer[SType] = new Decomposer[SType] {
    def decompose(ctype : SType) : JValue = JString(nameOf(ctype))
  }

  implicit val STypeExtractor : Extractor[SType] = new Extractor[SType] with ValidatedExtraction[SType] {
    override def validated(obj : JValue) : Validation[Error,SType] = 
      obj.validated[String].map( fromName _ ) match {
        case Success(Some(t)) => Success(t)
        case Success(None)    => Failure(Invalid("Unknown type."))
        case Failure(f)       => Failure(f)
      }
  }
}

object SType extends STypeSerialization {
  def forValue(jval: JValue): Option[SType] = jval match {
    case JBool(_)   => Some(SBoolean)
    case JInt(_)    => Some(SDecimalArbitrary)
    case JDouble(_) => Some(SDouble)
    case JString(_) => Some(SStringArbitrary)
    case JNull      => Some(SNull)
    case _          => None
  }
}

case object SObject extends SType with (Map[String, SValue] => SValue) {
  def format = CompositeFormat
  def apply(f: Map[String, SValue]) = new SValue {
    def fold[A](
      obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
      str:    String => A, bool:   Boolean => A,
      long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
      nul:    => A
    ) = obj(f)
  }

  def unapply(v: SValue): Option[Map[String, SValue]] = v.mapObjectOr(Option.empty[Map[String, SValue]])(Some(_))
}

case object SArray extends SType with (Vector[SValue] => SValue) {
  def format = CompositeFormat
  def apply(v: Vector[SValue]) = new SValue {
    def fold[A](
      obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
      str:    String => A, bool:   Boolean => A,
      long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
      nul:    => A
    ) = arr(v)
  }

  def unapply(v: SValue): Option[Vector[SValue]] = v.mapArrayOr(Option.empty[Vector[SValue]])(Some(_))
}

object SString extends (String => SValue) {
  def unapply(v: SValue): Option[String] = v.mapStringOr(Option.empty[String])(Some(_))

  def apply(v: String) = new SValue {
    def fold[A](
      obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
      str:    String => A, bool:   Boolean => A,
      long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
      nul:    => A
    ) = str(v)
  }
}

case class SStringFixed(width: Int) extends SType {
  def format = FixedWidth(width)  
}

case object SStringArbitrary extends SType {
  def format = LengthEncoded  
}

case object SBoolean extends SType with (Boolean => SValue) {
  def format = FixedWidth(1)
  def apply(v: Boolean) = new SValue {
    def fold[A](
      obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
      str:    String => A, bool:   Boolean => A,
      long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
      nul:    => A
    ) = bool(v)
  }
  def unapply(v: SValue): Option[Boolean] = v.mapBooleanOr(Option.empty[Boolean])(Some(_))
}

case object SInt extends SType with (Int => SValue) {
  def format = FixedWidth(4)
  def apply(v: Int) = new SValue {
    def fold[A](
      obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
      str:    String => A, bool:   Boolean => A,
      long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
      nul:    => A
    ) = long(v)
  }
}

case object SLong extends SType with (Long => SValue) {
  def format = FixedWidth(8)
  def apply(v: Long) = new SValue {
    def fold[A](
      obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
      str:    String => A, bool:   Boolean => A,
      long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
      nul:    => A
    ) = long(v)
  }
  def unapply(v: SValue): Option[Long] = v.mapLongOr(Option.empty[Long])(Some(_))
}

case object SFloat extends SType with (Float => SValue) {
  def format = FixedWidth(4)
  def apply(v: Float) = new SValue {
    def fold[A](
      obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
      str:    String => A, bool:   Boolean => A,
      long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
      nul:    => A
    ) = double(v)
  }
}

case object SDouble extends SType with (Double => SValue) {
  def format = FixedWidth(8)
  def apply(v: Double) = new SValue {
    def fold[A](
      obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
      str:    String => A, bool:   Boolean => A,
      long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
      nul:    => A
    ) = double(v)
  }
  def unapply(v: SValue): Option[Double] = v.mapDoubleOr(Option.empty[Double])(Some(_))
}

case object SDecimalArbitrary extends SType {
  def format = LengthEncoded  
}

object SDecimal extends (BigDecimal => SValue) {
  def unapply(v: SValue): Option[BigDecimal] = v.mapBigDecimalOr(Option.empty[BigDecimal])(Some(_))
  def apply(v: BigDecimal) = new SValue {
    def fold[A](
      obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
      str:    String => A, bool:   Boolean => A,
      long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
      nul:    => A
    ) = num(v)
  }
}

case object SNull extends SType with SValue {
  def format = FixedWidth(1)
  def fold[A](
    obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
    str:    String => A, bool:   Boolean => A,
    long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
    nul:    => A
  ) = nul
}

case object SEmptyObject extends SType with SValue {
  def format = FixedWidth(1)
  def fold[A](
    obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
    str:    String => A, bool:   Boolean => A,
    long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
    nul:    => A
  ) = obj(Map.empty)
}

case object SEmptyArray extends SType with SValue {
  def format = FixedWidth(1)
  def fold[A](
    obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
    str:    String => A, bool:   Boolean => A,
    long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
    nul:    => A
  ) = arr(Vector.empty)
}

