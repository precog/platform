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

import com.precog.bytecode._

object Schema {
  def ctypes(primitive : JPrimitiveType): Set[CType] = primitive match {
    case JNumberT => Set(CLong, CDouble, CNum)
    case JTextT => Set(CString)
    case JBooleanT => Set(CBoolean)
    case JNullT => Set(CNull)
  }
  /**
   * Constructs a JType corresponding to the supplied sequence of (JPath, CType) pairs. Returns None if the
   * supplied sequence is empty.
   */
  def mkType(ctpes: Seq[(JPath, CType)]): Option[JType] = {
    val primitives = ctpes.collect {
      case (JPath.Identity, CLong | CDouble | CNum) => JNumberT
      case (JPath.Identity, CString) => JTextT
      case (JPath.Identity, CBoolean) => JBooleanT
      case (JPath.Identity, CNull) => JNullT
      case (JPath.Identity, CEmptyArray) => JArrayFixedT(Map())
      case (JPath.Identity, CEmptyObject) => JObjectFixedT(Map())
    }

    val indices = ctpes.foldLeft(BitSet()) {
      case (acc, (JPath(JPathIndex(i), _*), _)) => acc+i
      case (acc, _) => acc
    }

    val elements = indices.flatMap { i =>
      mkType(ctpes.collect {
        case (JPath(JPathIndex(`i`), tail @ _*), ctpe) => (JPath(tail : _*), ctpe)
      }).map(i -> _)
    }
    val array = if (elements.isEmpty) Nil else List(JArrayFixedT(elements.toMap))

    val keys = ctpes.foldLeft(Set.empty[String]) {
      case (acc, (JPath(JPathField(key), _*), _)) => acc+key
      case (acc, _) => acc
    }

    val members = keys.flatMap { key =>
      mkType(ctpes.collect {
        case (JPath(JPathField(`key`), tail @ _*), ctpe) => (JPath(tail : _*), ctpe)
      }).map(key -> _)
    }
    val obj = if (members.isEmpty) Nil else List(JObjectFixedT(members.toMap))

    (primitives ++ array ++ obj).reduceOption(JUnionT)
  }

  /**
   * Tests whether the supplied JType includes the supplied JPath and CType.
   */
  def includes(jtpe: JType, path: JPath, ctpe: CType): Boolean = (jtpe, (path, ctpe)) match {
    case (JNumberT, (JPath.Identity, CLong | CDouble | CNum)) => true

    case (JTextT, (JPath.Identity, CString)) => true

    case (JBooleanT, (JPath.Identity, CBoolean)) => true

    case (JNullT, (JPath.Identity, CNull))=> true

    case (JObjectUnfixedT, (JPath.Identity, CEmptyObject)) => true
    case (JObjectUnfixedT, (JPath(JPathField(_), _*), _)) => true
    case (JObjectFixedT(fields), (JPath.Identity, CEmptyObject)) if fields.isEmpty => true

    case (JObjectFixedT(fields), (JPath(JPathField(head), tail @ _*), ctpe)) => {
      val fieldHead = fields.get(head)
      fields.get(head).map(includes(_, JPath(tail: _*), ctpe)).getOrElse(false)
    }

    case (JArrayUnfixedT, (JPath.Identity, CEmptyArray)) => true
    case (JArrayUnfixedT, (JPath(JPathIndex(_), _*), _)) => true
    case (JArrayFixedT(elements), (JPath.Identity, CEmptyArray)) if elements.isEmpty => true
    case (JArrayFixedT(elements), (JPath(JPathIndex(i), tail @ _*), ctpe)) =>
      elements.get(i).map(includes(_, JPath(tail: _*), ctpe)).getOrElse(false)

    case (JUnionT(ljtpe, rjtpe), (path, ctpe)) => includes(ljtpe, path, ctpe) || includes(rjtpe, path, ctpe)

    case _ => false
  }

  /**
   * Tests whether the supplied sequence contains all the (JPath, CType) pairs that are
   * included by the supplied JType.
   */
  def subsumes(ctpes: Seq[(JPath, CType)], jtpe: JType): Boolean = (jtpe, ctpes) match {
    case (JNumberT, ctpes) => ctpes.exists {
      case (JPath.Identity, CLong | CDouble | CNum) => true
      case _ => false
    }

    case (JTextT, ctpes) => ctpes.contains(JPath.Identity -> CString)

    case (JBooleanT, ctpes) => ctpes.contains(JPath.Identity -> CBoolean)

    case (JNullT, ctpes) => ctpes.contains(JPath.Identity -> CNull)

    case (JObjectFixedT(fields), ctpes) if fields.isEmpty => ctpes.contains(JPath.Identity -> CEmptyObject)
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

    case (JArrayFixedT(elements), ctpes) if elements.isEmpty => ctpes.contains(JPath.Identity -> CEmptyArray)
    case (JArrayUnfixedT, ctpes) => ctpes.exists {
      case (JPath(JPathIndex(_), _*), _) => true
      case _ => false
    }
    case (JArrayFixedT(elements), ctpes) => {
      val indices = elements.keySet
      indices.forall { i =>
        subsumes(
          ctpes.collect { case (JPath(JPathIndex(`i`), tail @ _*), ctpe) => (JPath(tail : _*), ctpe) }, 
          elements(i))
      }
    }

    case (JUnionT(ljtpe, rjtpe), ctpes) => subsumes(ctpes, ljtpe) || subsumes(ctpes, rjtpe)

    case _ => false
  }
}
