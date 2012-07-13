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

trait TypeInferencer extends DAG {
  import yggdrasil.{ SDecimal, SString }
  import yggdrasil.Schema._
  import instructions.{ DerefArray, DerefObject, Map2Cross, Map2CrossLeft, Map2CrossRight }
  import dag._

  def inferTypes(graph: DepGraph, jtpe : JType) : DepGraph = {

    def inferSplitTypes(split : Split) = split match {
      case Split(loc, spec, child) => Split(loc, spec, inferTypes(child, jtpe))
    }

    graph match {
      case r : Root => r

      case New(loc, parent) => New(loc, inferTypes(parent, jtpe))

      case LoadLocal(loc, parent, _) => LoadLocal(loc, parent, jtpe)

      case Operate(loc, op, parent) => Operate(loc, op, inferTypes(parent, jtpe))

      case Reduce(loc, red, parent) => Reduce(loc, red, inferTypes(parent, jtpe))

      case Morph1(loc, m, parent) => Morph1(loc, m, inferTypes(parent, jtpe))

      case Morph2(loc, m, left, right) => Morph2(loc, m, inferTypes(left, jtpe), inferTypes(right, jtpe))

      case Join(loc, instr, left, right) => (instr, right.value) match {
        case (Map2Cross(DerefObject) | Map2CrossLeft(DerefObject) | Map2CrossRight(DerefObject), Some(SString(str))) =>
          Join(loc, instr, inferTypes(left, JObjectFixedT(Map(str -> jtpe))), right)

        case (Map2Cross(DerefArray) | Map2CrossLeft(DerefArray) | Map2CrossRight(DerefArray), Some(SDecimal(d))) =>
          Join(loc, instr, inferTypes(left, JArrayFixedT(Map(d.toInt -> jtpe))), right)

        case _ => Join(loc, instr, inferTypes(left, jtpe), inferTypes(right, jtpe))
      }

      case Filter(loc, cross, target, boolean) => Filter(loc, cross, inferTypes(target, jtpe), inferTypes(boolean, jtpe))

      case Sort(parent, indices) => Sort(inferTypes(parent, jtpe), indices)

      case Memoize(parent, priority) => Memoize(inferTypes(parent, jtpe), priority)

      case Distinct(loc, parent) => Distinct(loc, inferTypes(parent, jtpe))

      case s : Split => inferSplitTypes(s)

      case s @ SplitGroup(loc, id, provenance) => SplitGroup(loc, id, provenance)(inferSplitTypes(s.parent))

      case s @ SplitParam(loc, id) => SplitParam(loc, id)(inferSplitTypes(s.parent))
    }
  }
}
