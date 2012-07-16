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
package com.precog
package daze

import bytecode.RandomLibrary
import common.Path
import yggdrasil._

import blueeyes.json.{ JPath, JPathField, JPathIndex }

import org.specs2.execute.Result
import org.specs2.mutable._

import scalaz._

class TypeInferencerSpec extends Specification
  with DAG
  with RandomLibrary
  with TypeInferencer {

  import dag._
  import instructions.{
    Add, DerefArray, DerefObject, Line, Map2Cross,
    Map2CrossLeft, Map2CrossRight, Map2Match, PushString
  }
  import JType._

  def flattenType(jtpe : JType) : Map[JPath, Set[CType]] = {
    def flattenAux(jtpe : JType) : Set[(JPath, CType)] = jtpe match {
      case p : JPrimitiveType => p.ctypes.map((JPath.Identity, _))

      case JArrayFixedT(elems) =>
        for((i, jtpe) <- elems.toSet; (path, ctpes) <- flattenAux(jtpe)) yield (JPathIndex(i) \ path, ctpes)

      case JObjectFixedT(fields) =>
        for((field, jtpe) <- fields.toSet; (path, ctpes) <- flattenAux(jtpe)) yield (JPathField(field) \ path, ctpes)

      case JUnionT(left, right) => flattenAux(left) ++ flattenAux(right)

      case _ => sys.error("TODO") // JArrayUnfixedT & JObjectUnfixedT
    }

    flattenAux(jtpe).groupBy(_._1).mapValues(_.map(_._2))
  }

  def extractLoads(graph : DepGraph) : Map[String, Map[JPath, Set[CType]]] = {
    graph match {
      case r : Root => Map.empty

      case New(loc, parent) => extractLoads(parent)

      case LoadLocal(loc, Root(_, PushString(path)), jtpe) => Map(path -> flattenType(jtpe))

      case Operate(loc, op, parent) => extractLoads(parent)

      case Reduce(loc, red, parent) => extractLoads(parent)

      case Morph1(loc, m, parent) => extractLoads(parent)

      case Morph2(loc, m, left, right) => extractLoads(left) ++ extractLoads(right)

      case Join(loc, instr, left, right) => extractLoads(left) ++ extractLoads(right)

      case Filter(loc, cross, target, boolean) => extractLoads(target) ++ extractLoads(boolean)

      case Sort(parent, indices) => extractLoads(parent)

      case Memoize(parent, priority) => extractLoads(parent)

      case Distinct(loc, parent) => extractLoads(parent)

      case Split(loc, spec, child) => extractLoads(child)

      case s @ SplitGroup(loc, id, provenance) => extractLoads(s.parent)

      case s @ SplitParam(loc, id) => extractLoads(s.parent)
    }
  }

  "type inference" should {
    "rewrite loads such that they will restrict the columns loaded" in {
      val line = Line(0, "")

      val input = Join(line, Map2Match(Add),
        Join(line, Map2Cross(DerefObject), 
          dag.LoadLocal(line, Root(line, PushString("/clicks"))),
          Root(line, PushString("time"))),
        Join(line, Map2Cross(DerefObject),
          dag.LoadLocal(line, Root(line, PushString("/hom/heightWeight"))),
          Root(line, PushString("height"))))

      val result = extractLoads(inferTypes(JPrimitiveUnfixedT)(input))

      val expected = Map(
        "/clicks" -> Map(JPath("time") -> Set(CBoolean, CStringArbitrary, CLong, CDouble, CDecimalArbitrary, CNull)),
        "/hom/heightWeight" -> Map(JPath("height") -> Set(CBoolean, CStringArbitrary, CLong, CDouble, CDecimalArbitrary, CNull))
      )

      result must_== expected
    }
  }
}

// vim: set ts=4 sw=4 et:
