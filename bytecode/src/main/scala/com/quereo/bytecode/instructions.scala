package com.quereo.bytecode

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
