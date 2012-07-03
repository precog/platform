package com.precog.bytecode

import scalaz.Scalaz._

trait Instructions extends Library {
  sealed trait Instruction { self =>
    import instructions._

    def operandStackDelta: (Int, Int) = self match {
      case Map1(_) => (1, 1)
      case Map2Match(_) => (2, 1)
      case Map2Cross(_) => (2, 1)
      case Map2CrossLeft(_) => (2, 1)
      case Map2CrossRight(_) => (2, 1)
        
      case Reduce(_) => (1, 1)
      case Morph1(_) => (1, 1)
      case Morph2(_) => (2, 1)
      
      case VUnion => (2, 1)
      case VIntersect => (2, 1)
      
      case IUnion => (2, 1)
      case IIntersect => (2, 1)
      case SetDifference => (2, 1)
      
      case FilterMatch(depth, Some(_)) => (2 + depth, 1)
      case FilterCross(depth, Some(_)) => (2 + depth, 1)
      case FilterCrossLeft(depth, Some(_)) => (2 + depth, 1)
      case FilterCrossRight(depth, Some(_)) => (2 + depth, 1)
      
      case FilterMatch(_, None) => (2, 1)
      case FilterCross(_, None) => (2, 1)
      case FilterCrossLeft(_, None) => (2, 1)
      case FilterCrossRight(_, None) => (2, 1)
      
      case Group(_) => (2, 1)
      case MergeBuckets(_) => (2, 1)
      case KeyPart(_) => (1, 1)
      case Extra => (1, 1)
      
      case Split => (1, 0)
      case Merge => (1, 1)
      
      case Dup => (1, 2)
      case Drop => (1, 0)
      case Swap(depth) => (depth + 1, depth + 1)
      
      case Line(_, _) => (0, 0)
      
      case LoadLocal(_) => (1, 1)
      
      case PushString(_) => (0, 1)
      case PushNum(_) => (0, 1)
      case PushTrue => (0, 1)
      case PushFalse => (0, 1)
      case PushNull => (0, 1)
      case PushObject => (0, 1)
      case PushArray => (0, 1)
      
      case PushGroup(_) => (0, 1)
      case PushKey(_) => (0, 1)
    }

    def predicateStackDelta: (Int, Int) = self match {
      case FilterMatch(_, Some(predicate)) => predicate.foldLeft((0, 0))(_ |+| _.predicateStackDelta)
      case FilterCross(_, Some(predicate)) => predicate.foldLeft((0, 0))(_ |+| _.predicateStackDelta)
      case _ => (0, 0)
    }
  }
  
  // namespace
  object instructions {
    type Predicate = Vector[PredicateInstr]
    
    sealed trait DataInstr extends Instruction
    
    sealed trait JoinInstr extends Instruction
    
    case class Map1(op: UnaryOperation) extends Instruction
    case class Map2Match(op: BinaryOperation) extends Instruction with JoinInstr
    case class Map2Cross(op: BinaryOperation) extends Instruction with JoinInstr
    case class Map2CrossLeft(op: BinaryOperation) extends Instruction with JoinInstr
    case class Map2CrossRight(op: BinaryOperation) extends Instruction with JoinInstr
    
    case class Reduce(red: BuiltInReduction) extends Instruction
    case class Morph1(m1: BuiltInMorphism) extends Instruction
    case class Morph2(m2: BuiltInMorphism) extends Instruction with JoinInstr
    
    case object VUnion extends Instruction with JoinInstr
    case object VIntersect extends Instruction with JoinInstr
    
    case object IUnion extends Instruction with JoinInstr
    case object IIntersect extends Instruction with JoinInstr
    case object SetDifference extends Instruction with JoinInstr
    
    case class Group(id: Int) extends Instruction
    case class MergeBuckets(and: Boolean) extends Instruction
    case class KeyPart(id: Int) extends Instruction
    case object Extra extends Instruction
    
    case object Split extends Instruction
    case object Merge extends Instruction
    
    case class FilterMatch(depth: Short, pred: Option[Predicate]) extends Instruction with DataInstr
    case class FilterCross(depth: Short, pred: Option[Predicate]) extends Instruction with DataInstr
    case class FilterCrossLeft(depth: Short, pred: Option[Predicate]) extends Instruction with DataInstr
    case class FilterCrossRight(depth: Short, pred: Option[Predicate]) extends Instruction with DataInstr
    
    case object Dup extends Instruction
    case object Drop extends Instruction
    case class Swap(depth: Int) extends Instruction with DataInstr
    
    case class Line(num: Int, text: String) extends Instruction with DataInstr
    
    case class LoadLocal(tpe: Type) extends Instruction
    
    sealed trait RootInstr extends Instruction
    
    case class PushString(str: String) extends Instruction with DataInstr with RootInstr
    case class PushNum(num: String) extends Instruction with DataInstr with RootInstr
    case object PushTrue extends Instruction with RootInstr
    case object PushFalse extends Instruction with RootInstr
    case object PushNull extends Instruction with RootInstr
    case object PushObject extends Instruction with RootInstr
    case object PushArray extends Instruction with RootInstr
    
    case class PushGroup(id: Int) extends Instruction
    case class PushKey(id: Int) extends Instruction
    
    sealed trait UnaryOperation
    sealed trait BinaryOperation
    
    sealed trait PredicateInstr { self =>
      def predicateStackDelta: (Int, Int) = self match {
        case Add => (2, 1)
        case Sub => (2, 1)
        case Mul => (2, 1)
        case Div => (2, 1)
        
        case Neg => (1, 1)
        
        case Or => (2, 1)
        case And => (2, 1)
        
        case Comp => (1, 1)
        
        case DerefObject => (1, 1)
        case DerefArray => (1, 1)
        
        case Range => (2, 1)
      }
    }
    
    sealed trait PredicateOp

    case class BuiltInMorphism(mor: Morphism)
    case class BuiltInFunction1Op(op: Op1) extends UnaryOperation
    case class BuiltInFunction2Op(op: Op2) extends BinaryOperation
    case class BuiltInReduction(red: Reduction)

    case object Add extends BinaryOperation with PredicateInstr with PredicateOp
    case object Sub extends BinaryOperation with PredicateInstr with PredicateOp
    case object Mul extends BinaryOperation with PredicateInstr with PredicateOp
    case object Div extends BinaryOperation with PredicateInstr with PredicateOp
    
    case object Lt extends BinaryOperation
    case object LtEq extends BinaryOperation
    case object Gt extends BinaryOperation
    case object GtEq extends BinaryOperation
    
    case object Eq extends BinaryOperation
    case object NotEq extends BinaryOperation
    
    case object Or extends BinaryOperation with PredicateInstr
    case object And extends BinaryOperation with PredicateInstr
    
    case object New extends UnaryOperation
    case object Comp extends UnaryOperation with PredicateInstr
    case object Neg extends UnaryOperation with PredicateInstr with PredicateOp
    
    case object WrapObject extends BinaryOperation
    case object WrapArray extends UnaryOperation
    
    case object JoinObject extends BinaryOperation
    case object JoinArray extends BinaryOperation
    
    case object ArraySwap extends BinaryOperation
    
    case object DerefObject extends BinaryOperation with PredicateInstr
    case object DerefArray extends BinaryOperation with PredicateInstr
    
    case object Range extends PredicateInstr

    sealed trait Type
    
    case object Het extends Type
  }
}
