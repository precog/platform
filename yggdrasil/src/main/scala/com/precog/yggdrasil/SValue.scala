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

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import scalaz._
import scalaz.Ordering._
import scalaz.syntax.order._
import scalaz.syntax.semigroup._
import scalaz.syntax.equal._
import scalaz.std.AllInstances._

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

  def asObject = mapObjectOr[Option[Map[String, SValue]]](None)(Some(_))
  def mapObjectOr[A](a: => A)(f: Map[String, SValue] => A): A = {
    val d = (_: Any) => a
    fold(f, d, d, d, d, d, d, a)
  }

  def asArray = mapArrayOr[Option[Vector[SValue]]](None)(Some(_))
  def mapArrayOr[A](a: => A)(f: Vector[SValue] => A): A = {
    val d = (_: Any) => a
    fold(d, f, d, d, d, d, d, a)
  }

  def asString = mapStringOr[Option[String]](None)(Some(_))
  def mapStringOr[A](a: => A)(f: String => A): A = {
    val d = (_: Any) => a
    fold(d, d, f, d, d, d, d, a)
  }

  def asBoolean = mapBooleanOr[Option[Boolean]](None)(Some(_))
  def mapBooleanOr[A](a: => A)(f: Boolean => A): A = {
    val d = (_: Any) => a
    fold(d, d, d, f, d, d, d, a)
  }

  def asLong = mapLongOr[Option[Long]](None)(Some(_))
  def mapLongOr[A](a: => A)(f: Long => A): A = {
    val d = (_: Any) => a
    fold(d, d, d, d, f, d, d, a)
  }

  def asDouble = mapDoubleOr[Option[Double]](None)(Some(_))
  def mapDoubleOr[A](a: => A)(f: Double => A): A = {
    val d = (_: Any) => a
    fold(d, d, d, d, d, f, d, a)
  }

  def asBigDecimal = mapBigDecimalOr[Option[BigDecimal]](None)(Some(_))
  def mapBigDecimalOr[A](a: => A)(f: BigDecimal => A): A = {
    val d = (_: Any) => a
    fold(d, d, d, d, d, d, f, a)
  }

  def isNull = mapNullOr(false)(true)
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

  def structure: Seq[(JPath, ColumnType)] = fold(
    obj = m => {
      if (m.isEmpty) List((JPath(), SEmptyObject))
      else {
        m.toSeq.flatMap { case (name, value) => value.structure map { case (path, ctype) => (JPathField(name) \ path, ctype) }}
      }
    },
    arr = a => {
      if (a.isEmpty) List((JPath(), SEmptyArray))
      else {
        a.zipWithIndex.flatMap { case (value, index) => value.structure map { case (path, ctype) => (JPathIndex(index) \ path, ctype) }}
      }
    },
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

  def merge(other: SValue): SValue = {
    mapObjectOr(
      mapArrayOr(sys.error("cannot merge non object/array values")) { arrElems =>
        other.mapArrayOr(sys.error("cannot merge an array with non-array")) { otherElems =>
          List(arrElems, otherElems).sortBy(_.length) match {
            case shorter :: longer :: Nil =>
              var i = 0
              var newElems = Vector.empty[SValue]
              while (i < shorter.length) newElems :+ (shorter(i) merge longer(i))
              while (i < longer.length)  newElems :+ longer(i)
              SArray(newElems)
          } 
        }
      }
    ) { objMap =>
      other.mapObjectOr(sys.error("cannot merge object with non-objecT")) { otherMap =>
        SObject(
          objMap.foldLeft(otherMap) {
            case (acc, (k, v)) => acc + (k -> acc.get(k).map(_ merge v).getOrElse(v))
          }
        )
      }
    }
  }

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
      num  = n => "SDecimal(" + n + ")",
      nul  = "SNull"
    )
}


trait SValueInstances {
  case class paired(sv1: SValue, sv2: SValue) {
    def fold[A](default: => A)(
      obj:    Map[String, SValue] => Map[String, SValue] => A,
      arr:    Vector[SValue] => Vector[SValue] => A,
      str:    String => String => A,
      bool:   Boolean => Boolean => A,
      long:   Long => Long => A,
      double: Double => Double => A,
      num:    BigDecimal => BigDecimal => A,
      nul:    => A
    ) = sv1.fold(
      obj    = v => sv2.mapObjectOr(default)(obj(v)),
      arr    = v => sv2.mapArrayOr(default)(arr(v)),
      str    = v => sv2.mapStringOr(default)(str(v)),
      bool   = v => sv2.mapBooleanOr(default)(bool(v)),
      long   = v => sv2.mapLongOr(default)(long(v)),
      double = v => sv2.mapDoubleOr(default)(double(v)),
      num    = v => sv2.mapBigDecimalOr(default)(num(v)),
      nul    = sv2.mapNullOr(default)(nul)
    )
  }

  def typeIndex(sv: SValue) = sv.fold(
    obj  = o => 7,
    arr  = a => 6,
    str  = s => 5,
    num  = n => 4,
    double = d => 3,
    long = l => 2,
    bool = b => 1,
    nul  = 0
  )

  implicit def order: Order[SValue] = new Order[SValue] {
    private val objectOrder = (o1: Map[String, SValue]) => (o2: Map[String, SValue]) => {
      (o1.size ?|? o2.size) |+| 
      (o1.toSeq.sortBy(_._1) zip o2.toSeq.sortBy(_._1)).foldLeft[Ordering](EQ) {
        case (ord, ((k1, v1), (k2, v2))) => ord |+| (k1 ?|? k2) |+| (v1 ?|? v2)
      }
    }

    private val arrayOrder = (o1: Vector[SValue]) => (o2: Vector[SValue]) => {
      (o1.length ?|? o2.length) |+| 
      (o1 zip o2).foldLeft[Ordering](EQ) {
        case (ord, (v1, v2)) => ord |+| (v1 ?|? v2)
      }
    }

    private val stringOrder = (Order[String].order _).curried
    private val boolOrder = (Order[Boolean].order _).curried
    private val longOrder = (Order[Long].order _).curried
    private val doubleOrder = (Order[Double].order _).curried
    private val numOrder = (Order[BigDecimal].order _).curried

    def order(sv1: SValue, sv2: SValue) = paired(sv1, sv2).fold(typeIndex(sv1) ?|? typeIndex(sv2))(
      obj    = objectOrder,
      arr    = arrayOrder,
      str    = stringOrder,
      bool   = boolOrder,
      long   = longOrder,
      double = doubleOrder,
      num    = numOrder,
      nul    = EQ
    )
  }

  implicit def equal: Equal[SValue] = new Equal[SValue] {
    private val objectEqual = (o1: Map[String, SValue]) => (o2: Map[String, SValue]) => 
      (o1.size == o2.size) &&
      (o1.toSeq.sortBy(_._1) zip o2.toSeq.sortBy(_._1)).foldLeft(true) {
        case (eql, ((k1, v1), (k2, v2))) => eql && k1 == k2 && v1 === v2
      }

    private val arrayEqual = (o1: Vector[SValue]) => (o2: Vector[SValue]) => 
      (o1.length == o2.length) &&
      (o1 zip o2).foldLeft(true) {
        case (eql, (v1, v2)) => eql && v1 === v2
      }

    private val stringEqual = (Equal[String].equal _).curried
    private val boolEqual = (Equal[Boolean].equal _).curried
    private val longEqual = (Equal[Long].equal _).curried
    private val doubleEqual = (Equal[Double].equal _).curried
    private val numEqual = (Equal[BigDecimal].equal _).curried

    def equal(sv1: SValue, sv2: SValue) = paired(sv1, sv2).fold(false)(
      obj    = objectEqual,
      arr    = arrayEqual,
      str    = stringEqual,
      bool   = boolEqual,
      long   = longEqual,
      double = doubleEqual,
      num    = numEqual,
      nul    = true
    )
  }
}

object SValue extends SValueInstances with BigIntHelpers {
  // Note this conversion has a peer for CValues that should always be changed
  // in conjunction with this mapping.
  @inline
  def fromJValue(jv: JValue): SValue = jv match {
    case JObject(fields) => SObject(fields map { case JField(name, v) => (name, fromJValue(v)) } toMap)
    case JArray(elements) => SArray(Vector(elements map fromJValue: _*))
    case JInt(i) => convertBigInt(i)
    case JDouble(d) => SDouble(d)
    case JString(s) => SString(s)
    case JBool(s) => SBoolean(s)
    case JNull => SNull
    case _ => sys.error("Fix JValue")
  }

  @inline
  private def convertBigInt(i: BigInt): SValue = {
    if(i.isValidInt) {
      SInt(i.intValue)
    } else if(isValidLong(i)) {
      SLong(i.longValue)
    } else {
      SDecimal(BigDecimal(i))
    }
  }

  def apply(selector: JPath, cv: CValue): SValue = {
    selector.nodes match {
      case JPathField(_) :: xs => SObject(Map()).set(selector, cv).get
      case JPathIndex(_) :: xs => SArray(Vector.empty[SValue]).set(selector, cv).get
      case Nil => cv.toSValue
    }
  }

  //TODO: Optimize
  def deref(selector: JPath): PartialFunction[SValue, SValue] = {
    case sv if (sv \ selector).isDefined => (sv \ selector).get
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

  def stype: SType
  
  def =~(tpe: SType): Boolean = (this, tpe) match {
    case (a, b) if a == b => true
    
    case (SStringFixed(_), SString) => true
    case (SStringArbitrary, SString) => true
    
    case (SInt, SDecimal) => true
    case (SLong, SDecimal) => true
    case (SFloat, SDecimal) => true
    case (SDouble, SDecimal) => true
    case (SDecimalArbitrary, SDecimal) => true
    
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
    case SEmptyObject           => "EmptyObject"
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

trait BigIntHelpers {
  
  private val MAX_LONG = BigInt(Long.MaxValue)
  private val MIN_LONG = BigInt(Long.MinValue)
  
  @inline
  protected final def isValidLong(i: BigInt): Boolean = {
    MIN_LONG <= i && i <= MAX_LONG
  }

}

object ColumnType extends ColumnTypeSerialization with BigIntHelpers {

  // Note this conversion has a peer for SValues that should always be changed
  // in conjunction with this mapping.
  @inline
  final def toCValue(jval: JValue): CValue = jval match {
    case JString(s) => CString(s)
    case JInt(i) => sizedIntCValue(i)
    case JDouble(d) => CDouble(d)
    case JBool(b) => CBoolean(b)
    case JNull => CNull
    case _ => sys.error("unpossible: " + jval.getClass.getName)
  }

  @inline
  final def forValue(jval: JValue): Option[ColumnType] = jval match {
    case JBool(_)   => Some(SBoolean)
    case JInt(bi)   => Some(sizedIntColumnType(bi))
    case JDouble(_) => Some(SDouble)
    case JString(_) => Some(SStringArbitrary)
    case JNull      => Some(SNull)
    case JArray(Nil) => Some(SEmptyArray)
    case JObject(Nil) => Some(SEmptyObject)
    case _          => None
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
  private final def sizedIntColumnType(bi: BigInt): ColumnType = {
   if(bi.isValidInt) {
      SInt 
    } else if(isValidLong(bi)) {
      SLong
    } else {
      SDecimalArbitrary
    }   
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
  override def toString(): String = "SObject"
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
  override def toString(): String = "SArray"
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
  override def toString(): String = "SString"
}

case class SStringFixed(width: Int) extends ColumnType {
  def format = FixedWidth(width)  
  val stype = SString
}

case object SStringArbitrary extends ColumnType {
  def format = LengthEncoded  
  val stype = SString
}

case object SBoolean extends SType with ColumnType with (Boolean => SValue) {
  def format = FixedWidth(1)
  val stype = this
  def apply(v: Boolean) = if (v) STrue else SFalse
  def unapply(v: SValue): Option[Boolean] = v.mapBooleanOr(Option.empty[Boolean])(Some(_))
  override def toString(): String = "SBoolean"
}

sealed trait SBooleanValue extends SValue {
  def &&(bool: SBooleanValue): SBooleanValue
  def ||(bool: SBooleanValue): SBooleanValue
}

case object STrue extends SValue with SBooleanValue {
  def fold[A](
    obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
    str:    String => A, bool:   Boolean => A,
    long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
    nul:    => A
  ) = bool(true)

  def &&(bool: SBooleanValue): SBooleanValue = bool
  def ||(bool: SBooleanValue): SBooleanValue = this
}

case object SFalse extends SValue with SBooleanValue {
  def fold[A](
    obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
    str:    String => A, bool:   Boolean => A,
    long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
    nul:    => A
  ) = bool(false)

  def &&(bool: SBooleanValue): SBooleanValue = this
  def ||(bool: SBooleanValue): SBooleanValue = bool
}


case object SInt extends ColumnType with (Int => SValue) {
  def format = FixedWidth(4)
  val stype = SDecimal
  def apply(v: Int) = new SValue {
    def fold[A](
      obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
      str:    String => A, bool:   Boolean => A,
      long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
      nul:    => A
    ) = long(v)
  }
  override def toString(): String = "SInt"
}

case object SLong extends ColumnType with (Long => SValue) {
  def format = FixedWidth(8)
  val stype = SDecimal
  def apply(v: Long) = new SValue {
    def fold[A](
      obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
      str:    String => A, bool:   Boolean => A,
      long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
      nul:    => A
    ) = long(v)
  }
  def unapply(v: SValue): Option[Long] = v.mapLongOr(Option.empty[Long])(Some(_))
  override def toString(): String = "SLong"
}

case object SFloat extends ColumnType with (Float => SValue) {
  def format = FixedWidth(4)
  val stype = SDecimal
  def apply(v: Float) = new SValue {
    def fold[A](
      obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
      str:    String => A, bool:   Boolean => A,
      long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
      nul:    => A
    ) = double(v)
  }
  override def toString(): String = "SFloat"
}

case object SDouble extends ColumnType with (Double => SValue) {
  def format = FixedWidth(8)
  val stype = SDecimal
  def apply(v: Double) = new SValue {
    def fold[A](
      obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
      str:    String => A, bool:   Boolean => A,
      long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
      nul:    => A
    ) = double(v)
  }
  def unapply(v: SValue): Option[Double] = v.mapDoubleOr(Option.empty[Double])(Some(_))
  override def toString(): String = "SDouble"
}

case object SDecimalArbitrary extends ColumnType {
  def format = LengthEncoded  
  val stype = SDecimal
}

case object SDecimal extends SType with (BigDecimal => SValue) {
  def unapply(v: SValue): Option[BigDecimal] = v.fold(
    obj = _ => None, arr = _ => None,
    str = _ => None, bool = _ => None,
    long = l => Some(BigDecimal(l)), double = d => Some(BigDecimal(d)), num = n => Some(n),
    nul = None
  )

  def apply(v: BigDecimal) = new SValue {
    def fold[A](
      obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
      str:    String => A, bool:   Boolean => A,
      long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
      nul:    => A
    ) = num(v)
  }
  override def toString(): String = "SDecimal"
}

case object SNull extends SType with ColumnType with SValue {
  def format = FixedWidth(0)
  val stype = this
  def fold[A](
    obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
    str:    String => A, bool:   Boolean => A,
    long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
    nul:    => A
  ) = nul
}

case object SEmptyObject extends ColumnType with SValue {
  def format = FixedWidth(0)
  val stype = SDecimal
  def fold[A](
    obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
    str:    String => A, bool:   Boolean => A,
    long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
    nul:    => A
  ) = obj(Map.empty)
}

case object SEmptyArray extends ColumnType with SValue {
  def format = FixedWidth(0)
  val stype = SDecimal
  def fold[A](
    obj:    Map[String, SValue] => A,   arr:    Vector[SValue] => A,
    str:    String => A, bool:   Boolean => A,
    long:   Long => A,   double: Double => A,  num:    BigDecimal => A,
    nul:    => A
  ) = arr(Vector.empty)
}

