package com.precog.yggdrasil

import com.precog.util._

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.xschema._
import blueeyes.json.xschema.Extractor._
import blueeyes.json.xschema.DefaultSerialization._

import scalaz._
import scalaz.Ordering._
import scalaz.syntax.order._
import scalaz.syntax.semigroup._
import scalaz.syntax.equal._
import scalaz.std.AllInstances._

sealed trait SValue {
  def isA(valueType: SType): Boolean 
  def hasProperty(selector: JPath) = (this \ selector).isDefined

  def \(selector: JPath): Option[SValue] = {
    if (selector == JPath.Identity) {
      Some(this)
    } else {
      this match {
        case SObject(obj) => 
          selector.nodes match {
            case JPathField(name) :: Nil => obj.get(name)
            case JPathField(name) :: xs  => obj.get(name).flatMap(_ \ JPath(xs)) 
          }

        case SArray(arr) => 
          selector.nodes match {
            case JPathIndex(i)    :: Nil => arr.lift(i) 
            case JPathIndex(i)    :: xs  => arr.lift(i).flatMap(_ \ JPath(xs)) 
          }

        case _ => None
      }
    }
  }

  def set(selector: JPath, value: SValue): Option[SValue] = this match {
    case SObject(obj) => 
      selector.nodes match {
        case JPathField(name) :: Nil => Some(SObject(obj + (name -> value))) 
        case JPathField(name) :: xs  => 
          val child = xs.head match { 
            case JPathField(_) => SObject.Empty
            case JPathIndex(_) => SArray.Empty
          }

          obj.getOrElse(name, child).set(JPath(xs), value).map(sv => (SObject(obj + (name -> sv)))) 
      }

    case SArray(arr) => 
      selector.nodes match {
        case JPathIndex(i) :: Nil => Some(SArray(arr.padTo(i + 1, SNull).updated(i, value))) 
        case JPathIndex(i) :: xs  => 
          val child = xs.head match { 
            case JPathField(_) => SObject.Empty
            case JPathIndex(_) => SArray.Empty
          }

          arr.lift(i).getOrElse(child).set(JPath(xs), value).map(sv => SArray(arr.padTo(i + 1, SNull).updated(i, sv))) 
      }

    case SNull if (selector == JPath.Identity) => Some(value)

    case _ => None
  }
  
  def set(selector: JPath, cv: CValue): Option[SValue] =
    if (cv eq null) None else set(selector, cv.toSValue)
  
  def structure: Seq[(JPath, CType)] = {
    import SValue._
    val s = this match {
      case SObject(m) =>
        if (m.isEmpty) List((JPath(), CEmptyObject))
        else {
          m.toSeq.flatMap { 
            case (name, value) => value.structure map { 
              case (path, ctype) => (JPathField(name) \ path, ctype) 
            }
          }
        }
      
      case SArray(a) =>
        if (a.isEmpty) List((JPath(), CEmptyArray))
        else {
          a.zipWithIndex.flatMap { 
            case (value, index) => value.structure map { 
              case (path, ctype) => (JPathIndex(index) \ path, ctype) 
            }
          }
        }
      
      case SString(_)     => List((JPath(), CStringArbitrary))
      case STrue | SFalse => List((JPath(), CBoolean))
      case SDecimal(_)    => List((JPath(), CDecimalArbitrary))
      case SNull          => List((JPath(), CNull)) 
    }

    s.sorted
  }
   
  lazy val shash: Long = structure.hashCode

  lazy val toJValue: JValue = this match {
    case SObject(obj) => JObject(obj.map({ case (k, v) => JField(k, v.toJValue) })(collection.breakOut))
    case SArray(arr)  => JArray(arr.map(_.toJValue)(collection.breakOut))
    case SString(s)   => JString(s)
    case STrue        => JBool(true)
    case SFalse       => JBool(false)
    case SDecimal(n)  => JDouble(n.toDouble) //sys.error("fix JValue"),
    case SNull        => JNull
  }
}


trait SValueInstances {
  case class paired(sv1: SValue, sv2: SValue) {
    assert(sv1 != null && sv2 != null)
    def fold[A](default: => A)(
      obj:    Map[String, SValue] => Map[String, SValue] => A,
      arr:    Vector[SValue] => Vector[SValue] => A,
      str:    String => String => A,
      bool:   Boolean => Boolean => A,
      num:    BigDecimal => BigDecimal => A,
      nul:    => A) = {
      (sv1, sv2) match {
        case (SObject(o1), SObject(o2)) => obj(o1)(o2)
        case (SArray(a1) , SArray(a2))  => arr(a1)(a2)
        case (SString(s1), SString(s2)) => str(s1)(s2)
        case (SBoolean(b1), SBoolean(b2)) => bool(b1)(b2)
        case (SDecimal(d1), SDecimal(d2)) => num(d1)(d2)
        case (SNull, SNull) => nul
        case _ => default
      }
    }
  }

  def typeIndex(sv: SValue) = sv match {
    case SObject(_)  => 7
    case SArray(_)   => 6
    case SString(_)  => 5
    case SDecimal(_) => 4
    case STrue | SFalse => 1
    case SNull       => 0
  }

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
      num    = numEqual,
      nul    = true
    )
  }

  implicit val StructureOrdering: scala.math.Ordering[(JPath, CType)] = implicitly[Order[(JPath, CType)]].toScalaOrdering
}

object SValue extends SValueInstances {
  // Note this conversion has a peer for CValues that should always be changed
  // in conjunction with this mapping.
   @inline
  def fromJValue(jv: JValue): SValue = jv match {
    case JObject(fields) => SObject(fields.map{ case JField(name, v) => (name, fromJValue(v)) }(collection.breakOut))
    case JArray(elements) => SArray((elements map fromJValue)(collection.breakOut))
    case JString(s) => SString(s)
    case JBool(s) => SBoolean(s)
    case JInt(i) => SDecimal(BigDecimal(i))
    case JDouble(d) => SDecimal(d)
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

  //TODO: Optimize
  def deref(selector: JPath): PartialFunction[SValue, SValue] = {
    case sv if (sv \ selector).isDefined => (sv \ selector).get
  }

  def asJSON(sv: SValue): String = pretty(render(sv.toJValue))
}


sealed trait SType {
  def =~(v: SValue): Boolean
  def =~(tpe: CType): Boolean = tpe =~ this
} 


case class SObject(fields: Map[String, SValue]) extends SValue {
  def isA(stype: SType) = stype == SObject
}

case object SObject extends SType {
  val Empty = SObject(Map())
  def =~(v: SValue): Boolean = v match {
    case SObject(_) => true
    case _ => false
  }
}


case class SArray(elements: Vector[SValue]) extends SValue {
  def isA(stype: SType) = stype == SArray
}

case object SArray extends SType  {
  val Empty = SArray(Vector())
  def =~(v: SValue): Boolean = v match {
    case SArray(_) => true
    case _ => false
  }
}


case class SString(value: String) extends SValue {
  def isA(stype: SType) = stype == SString
}

case object SString extends SType  {
  def =~(v: SValue): Boolean = v match {
    case SString(_) => true
    case _ => false
  }
}


case object SBoolean extends SType with (Boolean => SValue) {
  def apply(v: Boolean) = if (v) STrue else SFalse
  def unapply(v: SValue): Option[Boolean] = v match {
    case STrue  => Some(true)
    case SFalse => Some(false)
    case _ => None
  }

  def =~(v: SValue): Boolean = v match {
    case STrue | SFalse => true
    case _ => false
  }

  override def toString(): String = "SBoolean"
}

sealed trait SBooleanValue extends SValue {
  def &&(bool: SBooleanValue): SBooleanValue
  def ||(bool: SBooleanValue): SBooleanValue
  def isA(t: SType) = t == SBoolean
}

case object STrue extends SValue with SBooleanValue {
  def &&(bool: SBooleanValue): SBooleanValue = bool
  def ||(bool: SBooleanValue): SBooleanValue = this
}

case object SFalse extends SValue with SBooleanValue {
  def &&(bool: SBooleanValue): SBooleanValue = this
  def ||(bool: SBooleanValue): SBooleanValue = bool
}


case class SDecimal(value: BigDecimal) extends SValue {
  def isA(stype: SType) = stype == SDecimal
}

case object SDecimal extends SType {
  def =~(v: SValue): Boolean = v match {
    case SDecimal(_) => true
    case _ => false
  }
}


case object SNull extends SType with SValue {
  def isA(stype: SType) = stype == this
  def =~(v: SValue): Boolean = v match {
    case SNull => true
    case _ => false
  }
}

