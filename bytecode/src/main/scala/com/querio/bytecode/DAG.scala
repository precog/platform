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
package com.querio.bytecode

trait DAG extends Instructions {
  import instructions._
  
  sealed trait DepGraph {
    val loc: Line
  }
  
  object dag {
    case class Root(loc: Line, instr: RootInstr) extends DepGraph
    
    case class LoadLocal(loc: Line, ranges: Set[IndexRange], parent: DepGraph) extends DepGraph
    
    case class Operate(loc: Line, instr: OpInstr, parent: DepGraph) extends DepGraph
    case class Reduce(loc: Line, red: Reduction, parent: DepGraph) extends DepGraph
    
    case class Split(loc: Line, parent: DepGraph, child: DepGraph) extends DepGraph
    
    case class Join(loc: Line, instr: JoinInstr, left: DepGraph, right: DepGraph) extends DepGraph
    case class Filter(loc: Line, cross: Boolean, ranges: Set[IndexRange], target: DepGraph, boolean: DepGraph) extends DepGraph
    
    
    sealed trait IndexRange
    
    case class Conjunction(left: IndexRange, right: IndexRange) extends IndexRange
    case class Disjunction(left: IndexRange, right: IndexRange) extends IndexRange
    case class Complementation(child: IndexRange) extends IndexRange
    
    // [start, end)
    case class Contiguous(start: RangeOperand, end: RangeOperand) extends IndexRange
    
    
    sealed trait RangeOperand
    
    case class ValueOperand(source: DepGraph) extends RangeOperand
    case class PropertyOperand(source: DepGraph) extends RangeOperand
    case class IndexOperand(source: DepGraph) extends RangeOperand
    
    case class BinaryOperand(left: RangeOperand, op: PredicateOp with BinaryOperation, right: RangeOperand) extends RangeOperand
    case class UnaryOperand(op: PredicateOp with UnaryOperation, child: RangeOperand) extends RangeOperand
  }
}
