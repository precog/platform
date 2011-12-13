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

sealed trait Instruction

case class Map1(op: UnaryOperation) extends Instruction
case class Map2Match(op: BinaryOperation) extends Instruction
case class Map2CrossLeft(op: BinaryOperation) extends Instruction
case class Map2CrossRight(op: BinaryOperation) extends Instruction
case class Map2Cross(op: BinaryOperation) extends Instruction

case class Reduce(red: Reduction) extends Instruction

case object Union extends Instruction
case object Intersect extends Instruction

case class Filter(pred: Predicate) extends Instruction

case object Dup extends Instruction
case object Swap extends Instruction

case class LoadLocal(tpe: Type) extends Instruction

case class PushString(str: String) extends Instruction
case class PushNum(num: String) extends Instruction
case object PushTrue extends Instruction
case object PushFalse extends Instruction
case object PushObject extends Instruction
case object PushArray extends Instruction


sealed trait UnaryOperation
sealed trait BinaryOperation

case object Add extends BinaryOperation
case object Sub extends BinaryOperation
case object Mul extends BinaryOperation
case object Div extends BinaryOperation

case object Lt extends BinaryOperation
case object LtEq extends BinaryOperation
case object Gt extends BinaryOperation
case object GtEq extends BinaryOperation

case object Eq extends BinaryOperation
case object NotEq extends BinaryOperation

case object Or extends BinaryOperation
case object And extends BinaryOperation

case object Comp extends UnaryOperation
case object Neg extends UnaryOperation

case object WrapObject extends BinaryOperation
case object WrapArray extends UnaryOperation

case object JoinObject extends BinaryOperation
case object JoinArray extends BinaryOperation

case object DerefObject extends BinaryOperation
case object DerefArray extends BinaryOperation


sealed trait Reduction

case object Count extends Reduction

case object Mean extends Reduction
case object Median extends Reduction
case object Mode extends Reduction

case object Max extends Reduction
case object Min extends Reduction

case object StdDev extends Reduction
case object Sum extends Reduction


sealed trait Predicate


sealed trait Type

case object Het extends Type
