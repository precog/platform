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
import com.precog.common._

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.DefaultSerialization._

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
          (selector.nodes : @unchecked) match {
            case JPathField(name) :: Nil => obj.get(name)
            case JPathField(name) :: xs  => obj.get(name).flatMap(_ \ JPath(xs)) 
          }

        case SArray(arr) => 
          (selector.nodes : @unchecked) match {
            case JPathIndex(i)    :: Nil => arr.lift(i) 
            case JPathIndex(i)    :: xs  => arr.lift(i).flatMap(_ \ JPath(xs)) 
          }

        case _ => None
      }
    }
  }

  def set(selector: JPath, value: SValue): Option[SValue] = this match {
    case SObject(obj) => 
      (selector.nodes : @unchecked) match {
        case JPathField(name) :: Nil => Some(SObject(obj + (name -> value))) 
        case JPathField(name) :: xs  => 
          val child = xs.head match { 
            case JPathField(_) => SObject.Empty
            case JPathIndex(_) => SArray.Empty
          }

          obj.getOrElse(name, child).set(JPath(xs), value).map(sv => (SObject(obj + (name -> sv)))) 
      }

    case SArray(arr) => 
      (selector.nodes : @unchecked) match {
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
    if (cv eq null) None else set(selector, SValue.fromCValue(cv))
  
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
      
      case SString(_)     => List((JPath(), CString))
      case STrue | SFalse => List((JPath(), CBoolean))
      case SDecimal(_)    => List((JPath(), CNum))
      case SNull          => List((JPath(), CNull)) 
      case SUndefined     => List((JPath(), CUndefined))
    }

    s.sorted
  }
   
  lazy val shash: Long = structure.hashCode

  lazy val toJValue: JValue = this match {
    case SObject(obj) => JObject(obj.map({ case (k, v) => JField(k, v.toJValue) })(collection.breakOut))
    case SArray(arr)  => JArray(arr.map(_.toJValue)(collection.breakOut): _*)
    case SString(s)   => JString(s)
    case STrue        => JBool(true)
    case SFalse       => JBool(false)
    case SDecimal(n)  => JNum(n)
    case SNull        => JNull
    case SUndefined   => JUndefined
  }

  lazy val toRValue: RValue = this match {
    case SObject(obj) => RObject(obj.map({ case (k, v) => (k, v.toRValue) }))
    case SArray(arr)  => RArray(arr.map(_.toRValue)(collection.breakOut): _*)
    case SString(s)   => CString(s)
    case STrue        => CBoolean(true)
    case SFalse       => CBoolean(false)
    case SDecimal(n)  => CNum(n)
    case SNull        => CNull
    case SUndefined   => CUndefined
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
    case SUndefined       => 2
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

  implicit def scalaOrder: scala.math.Ordering[SValue] = order.toScalaOrdering

  implicit val StructureOrdering: scala.math.Ordering[(JPath, CType)] = implicitly[Order[(JPath, CType)]].toScalaOrdering
}

object SValue extends SValueInstances {
  @inline
  def fromCValue(cv: CValue): SValue = cv match {
    case CString(s) => SString(s)
    case CBoolean(b) => SBoolean(b)
    case CLong(n) => SDecimal(BigDecimal(n))
    case CDouble(n) => SDecimal(BigDecimal(n))
    case CNum(n) => SDecimal(n)
    case CDate(d) => sys.error("todo") // Should this be SString(d.toString)?
    case CPeriod(p) => sys.error("todo") // Should this be SString(d.toString)?
    case CArray(as, CArrayType(aType)) =>
      SArray(as.map(a => fromCValue(aType(a)))(collection.breakOut))
    case CNull => SNull
    case CEmptyArray => SArray(Vector())
    case CEmptyObject => SObject(Map())
    case CUndefined => SUndefined
  }

  // Note this conversion has a peer for CValues that should always be changed
  // in conjunction with this mapping.
   @inline
  def fromJValue(jv: JValue): SValue = jv match {
    case JObject(fields) => SObject(fields.map{ case JField(name, v) => (name, fromJValue(v)) }(collection.breakOut))
    case JArray(elements) => SArray((elements map fromJValue)(collection.breakOut))
    case JString(s) => SString(s)
    case JBool(s) => SBoolean(s)
    case JNum(d) => SDecimal(d)
    case JNull => SNull
    case _ => sys.error("Fix JValue")
  }

  def apply(selector: JPath, cv: CValue): SValue = {
    selector.nodes match {
      case JPathField(_) :: xs => SObject(Map()).set(selector, cv).get
      case JPathIndex(_) :: xs => SArray(Vector.empty[SValue]).set(selector, cv).get
      case Nil => SValue.fromCValue(cv)
    }
  }

  //TODO: Optimize
  def deref(selector: JPath): PartialFunction[SValue, SValue] = {
    case sv if (sv \ selector).isDefined => (sv \ selector).get
  }

  def asJSON(sv: SValue): String = sv.toJValue.renderPretty
}


sealed trait SType {
  def =~(v: SValue): Boolean
}

object SType {
  @inline
  def fromCType(ct: CType): SType = ct match {
    case CString => SString
    case CBoolean => SBoolean
    case (_: CNumericType[_]) => SDecimal
    case CDate => sys.error("todo")
    case CPeriod => sys.error("todo")
    case CNull => SNull
    case CArrayType(_) => SArray
    case CEmptyObject => SObject
    case CEmptyArray => SArray
    case CUndefined => SUndefined
  }
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

case object SString extends SType with (String => SString) {
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

case object SUndefined extends SType with SValue {
  def isA(stype: SType) = stype == this
  def =~(v: SValue): Boolean = v match {
    case SUndefined => true
    case _ => false
  }
}

