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

import scala.collection.immutable.BitSet

import blueeyes.json.{ JPath, JPathField, JPathIndex }

object Schema {
  sealed trait JType
  
  case object JNumberT extends JType
  case object JTextT extends JType
  case object JBooleanT extends JType
  case object JNullT extends JType
  
  sealed trait JArrayT extends JType
  case class JArrayFixedT(tpe: JType) extends JArrayT
  case object JArrayUnfixedT extends JArrayT

  sealed trait JObjectT extends JType
  case class JObjectFixedT(fields: Map[String, JType]) extends JObjectT
  case object JObjectUnfixedT extends JObjectT

  case class JUnionT(left: JType, right: JType) extends JType
  
  def flattenUnions(tpe: JType): Set[JType] = tpe match {
    case JUnionT(left, right) => flattenUnions(left) ++ flattenUnions(right)
    case t => Set(t)
  }

  def includes(jtpe : JType, path : JPath, ctpe : CType) : Boolean = (jtpe, (path, ctpe)) match {
    case (JNumberT, (JPath.Identity, CLong)) => true
    case (JNumberT, (JPath.Identity, CDouble)) => true
    case (JNumberT, (JPath.Identity, CDecimalArbitrary)) => true

    case (JTextT, (JPath.Identity, CStringFixed(_))) => true
    case (JTextT, (JPath.Identity, CStringArbitrary)) => true

    case (JBooleanT, (JPath.Identity, CBoolean)) => true

    case (JNullT, (JPath.Identity, CNull))=> true

    case (JObjectUnfixedT, (JPath.Identity, CEmptyObject)) => true
    case (JObjectUnfixedT, (JPath(JPathField(_), _*), _)) => true
    case (JObjectFixedT(fields), (JPath(JPathField(head), tail @ _*), ctpe)) =>
      fields.get(head).map(includes(_, JPath(tail : _*), ctpe)).getOrElse(false)

    case (JArrayUnfixedT, (JPath.Identity, CEmptyArray)) => true
    case (JArrayUnfixedT, (JPath(JPathIndex(_), _*), _)) => true
    case (JArrayFixedT(jtpe), (JPath(JPathIndex(_), tail @ _*), ctpe)) =>
      includes(jtpe, JPath(tail : _*), ctpe)

    case (JUnionT(ljtpe, rjtpe), (path, ctpe)) => includes(ljtpe, path, ctpe) || includes(rjtpe, path, ctpe)

    case _ => false
  }

  def subsumes(ctpes : List[(JPath, CType)], jtpe : JType) : Boolean = (jtpe, ctpes) match {
    case (JNumberT, ctpes) => ctpes.exists {
      case (JPath.Identity, CLong | CDouble | CDecimalArbitrary) => true
      case _ => false
    }

    case (JTextT, ctpes) => ctpes.exists {
      case (JPath.Identity, CStringFixed(_) | CStringArbitrary) => true
      case _ => false
    }

    case (JBooleanT, ctpes) => ctpes.contains(JPath.Identity, CBoolean)

    case (JNullT, ctpes) => ctpes.contains(JPath.Identity, CNull)

    case (JObjectUnfixedT, ctpes) if ctpes.contains(JPath.Identity, CEmptyObject) => true
    case (JObjectUnfixedT, ctpes) => ctpes.exists {
      case (JPath(JPathField(_), _*), _) => true
      case _ => false
    }
    case (JObjectFixedT(fields), ctpes) => {
      val keys = fields.keySet
      keys.forall { key =>
        subsumes(
          ctpes.collect { case (JPath(JPathField(`key`), tail @ _*), ctpe) => (JPath(tail : _*), ctpe) }, 
          fields(key))
      }
    }

    case (JArrayUnfixedT, ctpes) if ctpes.contains(JPath.Identity, CEmptyArray) => true
    case (JArrayUnfixedT, ctpes) => ctpes.exists {
      case (JPath(JPathIndex(_), _*), _) => true
      case _ => false
    }
    case (JArrayFixedT(jtpe), ctpes) => {
      val indices = ctpes.foldLeft(BitSet()) {
        case (acc, (JPath(JPathIndex(i), _*), _)) => acc + i
      }
      indices.forall { i =>
        subsumes(
          ctpes.collect { case (JPath(JPathIndex(`i`), tail @ _*), ctpe) => (JPath(tail : _*), ctpe) },
          jtpe)
      }
    }

    case (JUnionT(ljtpe, rjtpe), ctpes) => subsumes(ctpes, ljtpe) || subsumes(ctpes, rjtpe)

    case _ => false
  }
}
