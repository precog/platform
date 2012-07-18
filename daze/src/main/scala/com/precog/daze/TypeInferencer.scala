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

import bytecode._
import yggdrasil._

trait TypeInferencer extends DAG {
  import instructions.{
    BinaryOperation, ArraySwap, WrapArray, WrapObject, DerefArray, DerefObject,
    JoinInstr, Map2, Map2Cross, Map2CrossLeft, Map2CrossRight
  }
  import dag._

  def inferTypes(jtpe: JType)(graph: DepGraph) : DepGraph = {
    def inferSplitTypes(split: Split) = split match {
      case Split(loc, spec, child) => Split(loc, spec, inferTypes(jtpe)(child))
    }

    graph match {
      case r : Root => r

      case New(loc, parent) => New(loc, inferTypes(jtpe)(parent))

      case LoadLocal(loc, parent, _) => LoadLocal(loc, parent, jtpe)

      case Operate(loc, op, parent) => Operate(loc, op, inferTypes(op.tpe.arg)(parent))

      case Reduce(loc, red, parent) => Reduce(loc, red, inferTypes(red.tpe.arg)(parent))

      case Morph1(loc, m, parent) => Morph1(loc, m, inferTypes(m.tpe.arg)(parent))

      case Morph2(loc, m, left, right) => Morph2(loc, m, inferTypes(m.tpe.arg0)(left), inferTypes(m.tpe.arg1)(right))

      case Join(loc, instr @ (Map2Cross(DerefObject) | Map2CrossLeft(DerefObject) | Map2CrossRight(DerefObject)), left, right @ ConstString(str)) =>
        Join(loc, instr, inferTypes(JObjectFixedT(Map(str -> jtpe)))(left), right)

      case Join(loc, instr @ (Map2Cross(DerefArray) | Map2CrossLeft(DerefArray) | Map2CrossRight(DerefArray)), left, right @ ConstDecimal(d)) =>
        Join(loc, instr, inferTypes(JArrayFixedT(Map(d.toInt -> jtpe)))(left), right)

      case Join(loc, instr @ (Map2Cross(ArraySwap | WrapObject) | Map2CrossLeft(ArraySwap | WrapObject) | Map2CrossRight(ArraySwap | WrapObject)), left, right) =>
        Join(loc, instr, inferTypes(jtpe)(left), inferTypes(jtpe)(right))

      case Join(loc, instr @ Map2(BinaryOperationType(lhs, rhs, res)), left, right) => Join(loc, instr, inferTypes(lhs)(left), inferTypes(rhs)(right))

      case Join(loc, instr, left, right) => Join(loc, instr, inferTypes(jtpe)(left), inferTypes(jtpe)(right))

      case Filter(loc, cross, target, boolean) => Filter(loc, cross, inferTypes(jtpe)(target), inferTypes(JBooleanT)(boolean))

      case Sort(parent, indices) => Sort(inferTypes(jtpe)(parent), indices)

      case Memoize(parent, priority) => Memoize(inferTypes(jtpe)(parent), priority)

      case Distinct(loc, parent) => Distinct(loc, inferTypes(jtpe)(parent))

      case s : Split => inferSplitTypes(s)

      case s @ SplitGroup(loc, id, provenance) => SplitGroup(loc, id, provenance)(inferSplitTypes(s.parent))

      case s @ SplitParam(loc, id) => SplitParam(loc, id)(inferSplitTypes(s.parent))
    }
  }
}
