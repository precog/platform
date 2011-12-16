package com.querio.bytecode

trait Validator extends Instructions {
  import instructions._
  
  /**
   * Takes an instruction stream and returns the 
   */
  def validate(stream: Vector[Instruction]): Option[StackError] = {
    val (error, depth) = stream.foldLeft((None: Option[StackError], 0)) {
      case ((None, depth), inst) => {
        val (pop, push) = stackDelta(inst)
        
        inst match {
          case Swap(d) if d <= 0 =>
            (Some(NonPositiveSwapDepth(inst)), d)
          
          case FilterMatch(d, Some(_)) if d < 0 =>
            (Some(NegativeFilterDepth(inst)), depth)
          
          case FilterCross(d, Some(_)) if d < 0 =>
            (Some(NegativeFilterDepth(inst)), depth)
          
          case _ => {
            val predError = inst match {
              case FilterMatch(d, Some(pred)) => validatePredicate(inst, pred, d)
              case FilterCross(d, Some(pred)) => validatePredicate(inst, pred, d)
              case _ => None
            }
            
            if (depth < pop)
              (Some(StackUnderflow(inst)), depth - pop)
            else if (predError.isDefined)
              (predError, depth)
            else
              (None, depth - pop + push)
          }
        }
      }
      
      case (pair @ (Some(_), _), _) => pair
    }
    
    val excessError = if (depth != 1) Some(ExcessOperands) else None
    error orElse excessError
  }
  
  private def validatePredicate(origin: Instruction, pred: Predicate, depth: Int): Option[StackError] = {
    val (error, depth2) = pred.foldLeft((None: Option[StackError], depth)) {
      case ((None, depth), inst) => {
        val (pop, push) = predicateStackDelta(inst)
        
        if (depth < pop)
          (Some(PredicateStackUnderflow(origin)), depth - pop)
        else
          (None, depth - pop + push)
      }
      
      case (pair @ (Some(_), _), _) => pair
    }
    
    val excessError = if (depth2 != 1) Some(ExcessPredicateOperands(origin)) else None
    error orElse excessError
  }
  
  def stackDelta(inst: Instruction): (Int, Int) = inst match {
    case Map1(_) => (1, 1)
    case Map2Match(_) => (2, 1)
    case Map2CrossLeft(_) => (2, 1)
    case Map2CrossRight(_) => (2, 1)
    case Map2Cross(_) => (2, 1)
    
    case Reduce(_) => (1, 1)
    
    case VUnion => (2, 1)
    case VIntersect => (2, 1)
    
    case IUnion => (2, 1)
    case IIntersect => (2, 1)
    
    case FilterMatch(depth, Some(_)) => (2 + depth, 1)
    case FilterCross(depth, Some(_)) => (2 + depth, 1)
    
    case FilterMatch(_, None) => (2, 1)
    case FilterCross(_, None) => (2, 1)
    
    case Split => (1, 1)
    case Merge => (1, 1)
    
    case Dup => (1, 2)
    case Swap(depth) => (depth + 1, depth + 1)
    
    case Line(_, _) => (0, 0)
    
    case LoadLocal(_) => (1, 1)
    
    case PushString(_) => (0, 1)
    case PushNum(_) => (0, 1)
    case PushTrue => (0, 1)
    case PushFalse => (0, 1)
    case PushObject => (0, 1)
    case PushArray => (0, 1)
  }
  
  private def predicateStackDelta(inst: PredicateInstr): (Int, Int) = inst match {
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
  
  
  sealed trait StackError
  
  case class StackUnderflow(inst: Instruction) extends StackError
  case class PredicateStackUnderflow(inst: Instruction) extends StackError
  case object ExcessOperands extends StackError
  case class ExcessPredicateOperands(inst: Instruction) extends StackError
  
  case class NonPositiveSwapDepth(inst: Instruction) extends StackError
  case class NegativeFilterDepth(inst: Instruction) extends StackError
}
