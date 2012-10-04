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

import com.precog.common.json._

import com.precog.bytecode._

object Schema {
  def ctypes(primitive : JPrimitiveType): Set[CType] = primitive match {
    case JNumberT => Set(CLong, CDouble, CNum)
    case JTextT => Set(CString)
    case JBooleanT => Set(CBoolean)
    case JNullT => Set(CNull)
  }
  /**
   * Constructs a JType corresponding to the supplied sequence of (CPath, CType) pairs. Returns None if the
   * supplied sequence is empty.
   */
  def mkType(ctpes: Seq[(CPath, CType)]): Option[JType] = {
    val primitives = ctpes.collect {
      case (CPath.Identity, CLong | CDouble | CNum) => JNumberT
      case (CPath.Identity, CString) => JTextT
      case (CPath.Identity, CBoolean) => JBooleanT
      case (CPath.Identity, CNull) => JNullT
      case (CPath.Identity, CEmptyArray) => JArrayFixedT(Map())
      case (CPath.Identity, CEmptyObject) => JObjectFixedT(Map())
    }

    val elements = ctpes.collect {
      case (CPath(CPathIndex(i), _*), _) => i
    }.toSet.flatMap {
      (i: Int) =>
      mkType(ctpes.collect {
        case (CPath(CPathIndex(`i`), tail @ _*), ctpe) => (CPath(tail : _*), ctpe)
      }).map(i -> _)
    }
    val array = if (elements.isEmpty) Nil else List(JArrayFixedT(elements.toMap))

    val keys = ctpes.foldLeft(Set.empty[String]) {
      case (acc, (CPath(CPathField(key), _*), _)) => acc+key
      case (acc, _) => acc
    }

    val members = keys.flatMap { key =>
      mkType(ctpes.collect {
        case (CPath(CPathField(`key`), tail @ _*), ctpe) => (CPath(tail : _*), ctpe)
      }).map(key -> _)
    }
    val obj = if (members.isEmpty) Nil else List(JObjectFixedT(members.toMap))

    (primitives ++ array ++ obj).reduceOption(JUnionT)
  }

  /**
   * Tests whether the supplied JType includes the supplied CPath and CType.
   */
  def includes(jtpe: JType, path: CPath, ctpe: CType): Boolean = (jtpe, (path, ctpe)) match {
    case (JNumberT, (CPath.Identity, CLong | CDouble | CNum)) => true

    case (JTextT, (CPath.Identity, CString)) => true

    case (JBooleanT, (CPath.Identity, CBoolean)) => true

    case (JNullT, (CPath.Identity, CNull))=> true

    case (JObjectUnfixedT, (CPath.Identity, CEmptyObject)) => true
    case (JObjectUnfixedT, (CPath(CPathField(_), _*), _)) => true
    case (JObjectFixedT(fields), (CPath.Identity, CEmptyObject)) if fields.isEmpty => true

    case (JObjectFixedT(fields), (CPath(CPathField(head), tail @ _*), ctpe)) => {
      val fieldHead = fields.get(head)
      fields.get(head).map(includes(_, CPath(tail: _*), ctpe)).getOrElse(false)
    }

    case (JArrayUnfixedT, (CPath.Identity, CEmptyArray)) => true
    case (JArrayUnfixedT, (CPath(CPathIndex(_), _*), _)) => true
    case (JArrayFixedT(elements), (CPath.Identity, CEmptyArray)) if elements.isEmpty => true
    case (JArrayFixedT(elements), (CPath(CPathIndex(i), tail @ _*), ctpe)) =>
      elements.get(i).map(includes(_, CPath(tail: _*), ctpe)).getOrElse(false)

    case (JUnionT(ljtpe, rjtpe), (path, ctpe)) => includes(ljtpe, path, ctpe) || includes(rjtpe, path, ctpe)

    case _ => false
  }

  /**
   * Tests whether the supplied sequence contains all the (CPath, CType) pairs that are
   * included by the supplied JType.
   */
  def subsumes(ctpes: Seq[(CPath, CType)], jtpe: JType): Boolean = (jtpe, ctpes) match {
    case (JNumberT, ctpes) => ctpes.exists {
      case (CPath.Identity, CLong | CDouble | CNum) => true
      case _ => false
    }

    case (JTextT, ctpes) => ctpes.contains(CPath.Identity -> CString)

    case (JBooleanT, ctpes) => ctpes.contains(CPath.Identity -> CBoolean)

    case (JNullT, ctpes) => ctpes.contains(CPath.Identity -> CNull)

    case (JObjectFixedT(fields), ctpes) if fields.isEmpty => ctpes.contains(CPath.Identity -> CEmptyObject)
    case (JObjectUnfixedT, ctpes) => ctpes.exists {
      case (CPath(CPathField(_), _*), _) => true
      case _ => false
    }
    case (JObjectFixedT(fields), ctpes) => {
      val keys = fields.keySet
      keys.forall { key =>
        subsumes(
          ctpes.collect { case (CPath(CPathField(`key`), tail @ _*), ctpe) => (CPath(tail : _*), ctpe) }, 
          fields(key))
      }
    }

    case (JArrayFixedT(elements), ctpes) if elements.isEmpty => ctpes.contains(CPath.Identity -> CEmptyArray)
    case (JArrayUnfixedT, ctpes) => ctpes.exists {
      case (CPath(CPathIndex(_), _*), _) => true
      case _ => false
    }
    case (JArrayFixedT(elements), ctpes) => {
      val indices = elements.keySet
      indices.forall { i =>
        subsumes(
          ctpes.collect { case (CPath(CPathIndex(`i`), tail @ _*), ctpe) => (CPath(tail : _*), ctpe) }, 
          elements(i))
      }
    }

    case (JUnionT(ljtpe, rjtpe), ctpes) => subsumes(ctpes, ljtpe) || subsumes(ctpes, rjtpe)

    case _ => false
  }
}
