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

trait Instructions {
  sealed trait Instruction
  
  // namespace
  object instructions {
    type Predicate = Vector[PredicateInstr]
  
    private[instructions] class AbstractInstruction(
      val opstack: Int, val predstack: Int = 0, val vmstack: Int = 0
    ) extends Instruction
    
    sealed trait DataInstr extends Instruction
    
    case class Map1(op: UnaryOperation) extends AbstractInstruction(0)
    case class Map2Match(op: BinaryOperation) extends AbstractInstruction(-1)
    case class Map2CrossLeft(op: BinaryOperation) extends AbstractInstruction(-1)
    case class Map2CrossRight(op: BinaryOperation) extends AbstractInstruction(-1)
    case class Map2Cross(op: BinaryOperation) extends AbstractInstruction(-1)
    
    case class Reduce(red: Reduction) extends AbstractInstruction(0)
    
    case object VUnion extends AbstractInstruction(-1)
    case object VIntersect extends AbstractInstruction(-1)
    
    case object IUnion extends AbstractInstruction(-1)
    case object IIntersect extends AbstractInstruction(-1)
    
    case object Split extends AbstractInstruction(1, 0, 1)
    case object Merge extends AbstractInstruction(-1, 0, -1)
    
    case class FilterMatch(depth: Short, pred: Option[Predicate]) extends 
      AbstractInstruction(depth - 1) with DataInstr

    case class FilterCross(depth: Short, pred: Option[Predicate]) extends 
      AbstractInstruction(depth - 1) with DataInstr
    
    case object Dup extends AbstractInstruction(1)
    case class Swap(depth: Int) extends AbstractInstruction(0) with DataInstr
    
    case class Line(num: Int, text: String) extends AbstractInstruction(0) with DataInstr
    
    case class LoadLocal(tpe: Type) extends AbstractInstruction(1)
    
    case class PushString(str: String) extends AbstractInstruction(1) with DataInstr
    case class PushNum(num: String) extends AbstractInstruction(1) with DataInstr
    case object PushTrue extends AbstractInstruction(1)
    case object PushFalse extends AbstractInstruction(1)
    case object PushObject extends AbstractInstruction(1)
    case object PushArray extends AbstractInstruction(1)
    
    
    sealed trait UnaryOperation
    sealed trait BinaryOperation
    
    sealed trait PredicateInstr
    
    case object Add extends BinaryOperation with PredicateInstr
    case object Sub extends BinaryOperation with PredicateInstr
    case object Mul extends BinaryOperation with PredicateInstr
    case object Div extends BinaryOperation with PredicateInstr
    
    case object Lt extends BinaryOperation
    case object LtEq extends BinaryOperation
    case object Gt extends BinaryOperation
    case object GtEq extends BinaryOperation
    
    case object Eq extends BinaryOperation
    case object NotEq extends BinaryOperation
    
    case object Or extends BinaryOperation with PredicateInstr
    case object And extends BinaryOperation with PredicateInstr
    
    case object Comp extends UnaryOperation with PredicateInstr
    case object Neg extends UnaryOperation with PredicateInstr
    
    case object WrapObject extends BinaryOperation
    case object WrapArray extends UnaryOperation
    
    case object JoinObject extends BinaryOperation
    case object JoinArray extends BinaryOperation
    
    case object DerefObject extends BinaryOperation with PredicateInstr
    case object DerefArray extends BinaryOperation with PredicateInstr
    
    case object Range extends PredicateInstr
    
    
    sealed trait Reduction
    
    case object Count extends Reduction
    
    case object Mean extends Reduction
    case object Median extends Reduction
    case object Mode extends Reduction
    
    case object Max extends Reduction
    case object Min extends Reduction
    
    case object StdDev extends Reduction
    case object Sum extends Reduction
    
    
    sealed trait Type
    
    case object Het extends Type
  }
}
