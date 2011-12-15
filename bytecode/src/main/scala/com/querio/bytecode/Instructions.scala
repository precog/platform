package com.querio.bytecode

trait Instructions {
  sealed trait Instruction
  
  // namespace
  object instructions {
    type Predicate = Vector[PredicateInstr]
    
    sealed trait DataInstr extends Instruction
    
    case class Map1(op: UnaryOperation) extends Instruction
    case class Map2Match(op: BinaryOperation) extends Instruction
    case class Map2CrossLeft(op: BinaryOperation) extends Instruction
    case class Map2CrossRight(op: BinaryOperation) extends Instruction
    case class Map2Cross(op: BinaryOperation) extends Instruction
    
    case class Reduce(red: Reduction) extends Instruction
    
    case object VUnion extends Instruction
    case object VIntersect extends Instruction
    
    case object IUnion extends Instruction
    case object IIntersect extends Instruction
    
    case object Split extends Instruction
    case object Merge extends Instruction
    
    case class FilterMatch(depth: Int, pred: Predicate) extends Instruction with DataInstr
    case class FilterCross(depth: Int, pred: Predicate) extends Instruction with DataInstr
    
    case object Dup extends Instruction
    case class Swap(depth: Int) extends Instruction with DataInstr
    
    case class Line(num: Int, text: String) extends Instruction with DataInstr
    
    case class LoadLocal(tpe: Type) extends Instruction
    
    case class PushString(str: String) extends Instruction with DataInstr
    case class PushNum(num: String) extends Instruction with DataInstr
    case object PushTrue extends Instruction
    case object PushFalse extends Instruction
    case object PushObject extends Instruction
    case object PushArray extends Instruction
    
    
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
