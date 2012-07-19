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
      
      case FilterMatch => (2, 1)
      case FilterCross => (2, 1)
      case FilterCrossLeft => (2, 1)
      case FilterCrossRight => (2, 1)
      
      case FilterMatch => (2, 1)
      case FilterCross => (2, 1)
      case FilterCrossLeft => (2, 1)
      case FilterCrossRight => (2, 1)
      
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
      
      case LoadLocal => (1, 1)
      case Distinct => (1, 1)
      
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
  }
  
  // namespace
  object instructions {
    sealed trait DataInstr extends Instruction
    
    sealed trait JoinInstr extends Instruction
    
    case class Map1(op: UnaryOperation) extends Instruction
    case class Map2Match(op: BinaryOperation) extends Instruction with JoinInstr
    case class Map2Cross(op: BinaryOperation) extends Instruction with JoinInstr
    case class Map2CrossLeft(op: BinaryOperation) extends Instruction with JoinInstr
    case class Map2CrossRight(op: BinaryOperation) extends Instruction with JoinInstr
    
    case class Reduce(red: BuiltInReduction) extends Instruction
    case class Morph1(m1: BuiltInMorphism) extends Instruction
    case class Morph2(m2: BuiltInMorphism) extends Instruction
    
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
    
    case object FilterMatch extends Instruction with DataInstr
    case object FilterCross extends Instruction with DataInstr
    case object FilterCrossLeft extends Instruction with DataInstr
    case object FilterCrossRight extends Instruction with DataInstr
    
    case object Dup extends Instruction
    case object Drop extends Instruction
    case class Swap(depth: Int) extends Instruction with DataInstr
    
    case class Line(num: Int, text: String) extends Instruction with DataInstr
    
    case object LoadLocal extends Instruction
    case object Distinct extends Instruction
    
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
    
    object Map2 {
      def unapply(instr: JoinInstr): Option[BinaryOperationType] = instr match {
        case Map2Match(op) => Some(op.tpe)
        case Map2Cross(op) => Some(op.tpe) 
        case Map2CrossLeft(op) => Some(op.tpe)
        case Map2CrossRight(op) => Some(op.tpe)
        case _ => None
      }
    }
    
    sealed trait UnaryOperation {
      val tpe : UnaryOperationType
    }
    
    trait NumericUnaryOperation extends UnaryOperation {
      val tpe = UnaryOperationType(JNumberT, JNumberT)
    }

    trait BooleanUnaryOperation extends UnaryOperation {
      val tpe = UnaryOperationType(JBooleanT, JBooleanT)
    }

    trait UnfixedUnaryOperation extends UnaryOperation {
      val tpe = UnaryOperationType(JType.JUnfixedT, JType.JUnfixedT)
    }

    sealed trait BinaryOperation {
      val tpe : BinaryOperationType
    }
    
    trait NumericBinaryOperation extends BinaryOperation {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
    }
    
    trait NumericComparisonOperation extends BinaryOperation {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JBooleanT)
    }

    trait BooleanBinaryOperation extends BinaryOperation {
      val tpe = BinaryOperationType(JBooleanT, JBooleanT, JBooleanT)
    }

    trait EqualityOperation extends BinaryOperation {
      val tpe = BinaryOperationType(JBooleanT, JBooleanT, JBooleanT)
    }
    
    trait UnfixedBinaryOperation extends BinaryOperation {
      val tpe = BinaryOperationType(JType.JUnfixedT, JType.JUnfixedT, JBooleanT)
    }
    
    case class BuiltInFunction1Op(op: Op1) extends UnaryOperation {
      val tpe = op.tpe
    }
    case class BuiltInFunction2Op(op: Op2) extends BinaryOperation {
      val tpe = op.tpe
    }
    case class BuiltInMorphism(mor: Morphism)
    case class BuiltInReduction(red: Reduction)

    case object Add extends NumericBinaryOperation
    case object Sub extends NumericBinaryOperation
    case object Mul extends NumericBinaryOperation
    case object Div extends NumericBinaryOperation
    
    case object Lt extends NumericComparisonOperation
    case object LtEq extends NumericComparisonOperation
    case object Gt extends NumericComparisonOperation
    case object GtEq extends NumericComparisonOperation
    
    case object Eq extends EqualityOperation
    case object NotEq extends EqualityOperation
    
    case object Or extends BooleanBinaryOperation
    case object And extends BooleanBinaryOperation
    
    case object New extends UnfixedUnaryOperation
    case object Comp extends BooleanUnaryOperation
    case object Neg extends NumericUnaryOperation
    
    case object WrapObject extends UnfixedBinaryOperation
    case object WrapArray extends UnfixedUnaryOperation
    
    case object JoinObject extends UnfixedBinaryOperation
    case object JoinArray extends UnfixedBinaryOperation
    
    case object ArraySwap extends UnfixedBinaryOperation
    
    case object DerefObject extends UnfixedBinaryOperation
    case object DerefArray extends UnfixedBinaryOperation
    
    case object Range
  }
}
