package com.precog.yggdrasil

import blueeyes.json._
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

  def mapNullOr[A](a: => A)(ifNull: => A): A = {
    val d = (_: Any) => a
    fold(d, d, d, d, d, d, d, ifNull)
  }

  def hasProperty(selector: JPath) = (this \ selector).isDefined

  def \(selector: JPath): Option[SValue] = selector.nodes match {
    case JPathField(name) :: Nil => mapObjectOr(Option.empty[SValue]) { _.get(name) }
    case JPathField(name) :: xs  => mapObjectOr(Option.empty[SValue]) { _.get(name).flatMap(_ \ JPath(xs)) }
    case JPathIndex(i)    :: Nil => mapArrayOr(Option.empty[SValue])  { _.lift(i) }
    case JPathIndex(i)    :: xs  => mapArrayOr(Option.empty[SValue])  { _.lift(i).flatMap(_ \ JPath(xs)) }
    case Nil => Option.empty[SValue]
  }

  def set(selector: JPath, cv: CValue): Option[SValue] = {
    selector.nodes match {
      case JPathField(name) :: Nil => mapObjectOr(Option.empty[SValue])  { o => Some(SObject(o + (name -> cv.toSValue))) }
      case JPathField(name) :: xs  => mapObjectOr(Option.empty[SValue])  { o => 
                                                          val child = xs.head match { 
                                                            case JPathField(_)  => SObject(Map())
                                                            case JPathIndex(_) => SArray(Vector.empty[SValue])
                                                          }

                                                          o.getOrElse(name, child).set(JPath(xs), cv)
                                                          .map(sv => (SObject(o + (name -> sv)))) 
                                                        }

      case JPathIndex(i) :: Nil => mapArrayOr(Option.empty[SValue]) { a => Some(SArray(a.padTo(i + 1, SNull).updated(i, cv.toSValue))) }
      case JPathIndex(i) :: xs  => mapArrayOr(Option.empty[SValue]) { a => 
                                                      val child = xs.head match { 
                                                        case JPathField(_)  => SObject(Map())
                                                        case JPathIndex(_) => SArray(Vector.empty[SValue])
                                                      }

                                                      a.lift(i).getOrElse(child).set(JPath(xs), cv)
                                                      .map(sv => SArray(a.padTo(i + 1, SNull).updated(i, sv))) 
                                                    }
      case Nil => Some(cv.toSValue)
    }
  }

  lazy val structure: Seq[(JPath, ColumnType)] = fold(
    obj = m => m.toSeq.flatMap { case (name, value) => value.structure map { case (path, ctype) => (JPathField(name) \ path, ctype) }},
    arr = a => a.zipWithIndex.flatMap { case (value, index) => value.structure map { case (path, ctype) => (JPathIndex(index) \ path, ctype) }},
    str = s => List((JPath(), SStringArbitrary)),
    bool = b => List((JPath(), SBoolean)),
    long = l => List((JPath(), SLong)),
    double = d => List((JPath(), SDouble)),
    num = n => List((JPath(), SDecimalArbitrary)),
    nul = List((JPath(), SNull)) )
   
  lazy val shash: Long = structure.hashCode

  lazy val toJValue: JValue = fold(
    obj = obj => JObject(obj.map({ case (k, v) => JField(k, v.toJValue) })(collection.breakOut)),
    arr = arr => JArray(arr.map(_.toJValue)(collection.breakOut)),
    str = JString(_),
    bool = JBool(_),
    long = JInt(_),
    double = JDouble(_),
    num = n => JDouble(n.toDouble), //sys.error("fix JValue"),
    nul = JNull)

  abstract override def equals(obj: Any) = obj match {
    case sv: SValue => fold(
        obj  = o => sv.mapObjectOr(false)(_ == o),
        arr  = a => sv.mapArrayOr(false)(_ == a),
        str  = s => sv.mapStringOr(false)(_ == s),
        bool = b => sv.mapBooleanOr(false)(_ == b),
        long = l => sv.mapLongOr(false)(_ == l),
        double = d => sv.mapDoubleOr(false)(_ == d),
        num  = n => sv.mapBigDecimalOr(false)(_ == n),
        nul  = sv.mapNullOr(false)(true)
      )
    case _ => false 
  }

  abstract override def hashCode = 
    fold(
      obj  = o => o.hashCode,
      arr  = a => a.hashCode,
      str  = s => s.hashCode,
      bool = b => b.hashCode,
      long = l => l.hashCode,
      double = d => d.hashCode,
      num  = n => n.hashCode,
      nul  = 0x00
    )

  abstract override def toString = 
    fold(
      obj  = o => "SObject(" + o + ")",
      arr  = a => "SArray(" + a + ")",
      str  = s => "SString(" + s + ")",
      bool = b => "SBool(" + b + ")",
      long = l => "SLong(" + l + ")",
      double = d => "SDouble(" + d + ")",
      num  = n => "SNumeric(" + n + ")",
      nul  = "SNull"
    )
}

trait SValueInstances {
  implicit def equal: Equal[SValue] = new Equal[SValue] {
    def equal(sv1: SValue, sv2: SValue) = {
      (sv1 eq sv2) || sv1.fold(
        obj    = v => sv2.mapObjectOr(false)(_ == v),
        arr    = v => sv2.mapArrayOr(false)(_ == v),
        str    = v => sv2.mapStringOr(false)(_ == v),
        bool   = v => sv2.mapBooleanOr(false)(_ == v),
        long   = v => sv2.mapLongOr(false)(_ == v),
        double = v => sv2.mapDoubleOr(false)(_ == v),
        num    = v => sv2.mapBigDecimalOr(false)(_ == v),
        nul    = sv2.mapNullOr(false)(true)
      )
    }
  }
}

object SValue extends SValueInstances {
  def fromJValue(jv: JValue): SValue = jv match {
    case JObject(fields) => SObject(fields map { case JField(name, v) => (name, fromJValue(v)) } toMap)
    case JArray(elements) => SArray(Vector(elements map fromJValue: _*))
    case JInt(i) => SDecimal(BigDecimal(i))
    case JDouble(d) => SDouble(d)
    case JString(s) => SString(s)
    case JBool(s) => SBoolean(s)
    case JNull => SNull
    case _ => sys.error("Fix JValue")
  }

  def apply(selector: JPath, cv: CValue): SValue = {
    selector.nodes match {
      case JPathField(_) :: xs => SObject(Map()).set(selector, cv).get
      case JPathIndex(_) :: xs => SArray(Vector.empty[SValue]).set(selector, cv).get
      case Nil => cv.toSValue
    }
  }

  def asJSON(sv: SValue): String = sv.fold(
    obj = { obj =>
      val contents = obj mapValues asJSON map {
        case (key, value) => "\"%s\":%s".format(key, value)
      }
      
      "{%s}".format(contents mkString ",")
    },
    arr = { arr =>
      arr map asJSON mkString ("[", ",", "]")
    },
    str = { str => "\"%s\"".format(str) },     // TODO escaping
    bool = { b => b.toString },
    long = { i => i.toString },
    double = { d => d.toString },
    num = { d => d.toString },
    nul = "null")
}

sealed trait SType {
  def =~(tpe: ColumnType) = tpe =~ this
} 

sealed trait StorageFormat {
  def min(i: Int): Int
}

case object LengthEncoded extends StorageFormat {
  def min(i: Int) = i
}

case class  FixedWidth(width: Int) extends StorageFormat {
  def min(i: Int) = width min i
}

sealed trait ColumnType {
  def format: StorageFormat
  
  def =~(tpe: SType): Boolean = (this, tpe) match {
    case (a, b) if a == b => true
    
    case (SStringFixed(_), SString) => true
    case (SStringArbitrary, SString) => true
    
    case (SInt, SDecimal) => true
    case (SLong, SDecimal) => true
    case (SFloat, SDecimal) => true
    case (SDouble, SDecimal) => true
    case (SDecimalArbitrary, SDecimal) => true
    
    case (SEmptyObject, SObject) => true
    case (SEmptyArray, SArray) => true
    
    case (_, _) => false
  }
}

trait ColumnTypeSerialization {
  def nameOf(c: ColumnType): String = c match {
    case SStringFixed(width)    => "String("+width+")"
    case SStringArbitrary       => "String"
    case SBoolean               => "Boolean"
    case SInt                   => "Int"
    case SLong                  => "Long"
    case SFloat                 => "Float"
    case SDouble                => "Double"
    case SDecimalArbitrary      => "Decimal"
    case SNull                  => "Null"
    case SEmptyObject           => "SEmptyObject"
    case SEmptyArray            => "EmptyArray"
  } 

  def fromName(n: String): Option[ColumnType] = {
    val FixedStringR = """String\(\d+\)""".r
    n match {
      case FixedStringR(w)      => Some(SStringFixed(w.toInt))
      case "String"      => Some(SStringArbitrary)
      case "Boolean"     => Some(SBoolean)
      case "Int"         => Some(SInt)
      case "Long"        => Some(SLong)
      case "Float"       => Some(SFloat)
      case "Double"      => Some(SDouble)
      case "Decimal"     => Some(SDecimalArbitrary)
      case "Null"        => Some(SNull)
      case "EmptyObject" => Some(SEmptyObject)
      case "EmptyArray"  => Some(SEmptyArray)
      case _ => None
    }
  }
    
  implicit val PrimtitiveTypeDecomposer : Decomposer[ColumnType] = new Decomposer[ColumnType] {
    def decompose(ctype : ColumnType) : JValue = JString(nameOf(ctype))
  }

  implicit val STypeExtractor : Extractor[ColumnType] = new Extractor[ColumnType] with ValidatedExtraction[ColumnType] {
    override def validated(obj : JValue) : Validation[Error,ColumnType] = 
      obj.validated[String].map( fromName _ ) match {
        case Success(Some(t)) => Success(t)
        case Success(None)    => Failure(Invalid("Unknown type."))
        case Failure(f)       => Failure(f)
      }
  }
}

object ColumnType extends ColumnTypeSerialization {
  def forValue(jval: JValue): Option[ColumnType] = jval match {
    case JBool(_)   => Some(SBoolean)
    case JInt(_)    => Some(SDecimalArbitrary)
    case JDouble(_) => Some(SDouble)
    case JString(_) => Some(SStringArbitrary)
    case JNull      => Some(SNull)
    case JArray(Nil) => Some(SEmptyArray)
    case JObject(Nil) => Some(SEmptyObject)
    case _          => None
  }
}

case object SObject extends SType with (Map[String, SValue] => SValue) {
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

case object SString extends SType with (String => SValue) {
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

case class SStringFixed(width: Int) extends ColumnType {
  def format = FixedWidth(width)  
}

case object SStringArbitrary extends ColumnType {
  def format = LengthEncoded  
}

case object SBoolean extends SType with ColumnType with (Boolean => SValue) {
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

case object SInt extends SType with ColumnType with (Int => SValue) {
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

case object SLong extends SType with ColumnType with (Long => SValue) {
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

case object SFloat extends SType with ColumnType with (Float => SValue) {
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

case object SDouble extends SType with ColumnType with (Double => SValue) {
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

case object SDecimalArbitrary extends ColumnType {
  def format = LengthEncoded  
}

case object SDecimal extends SType with (BigDecimal => SValue) {
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

case object SNull extends SType with ColumnType with SValue {
  def format = FixedWidth(0)
  def fold[A](
    obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
    str:    String => A, bool:   Boolean => A,
    long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
    nul:    => A
  ) = nul
}

case object SEmptyObject extends SType with ColumnType with SValue {
  def format = FixedWidth(0)
  def fold[A](
    obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
    str:    String => A, bool:   Boolean => A,
    long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
    nul:    => A
  ) = obj(Map.empty)
}

case object SEmptyArray extends SType with ColumnType with SValue {
  def format = FixedWidth(0)
  def fold[A](
    obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
    str:    String => A, bool:   Boolean => A,
    long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
    nul:    => A
  ) = arr(Vector.empty)
}

