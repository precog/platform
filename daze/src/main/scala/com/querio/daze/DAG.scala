package com.querio
package daze

import bytecode._

trait DAG extends Instructions {
  import instructions._
  
  def decorate(stream: Vector[Instruction]): Either[StackError, DepGraph] = {
    import dag._
    
    def adjustSplits(roots: List[DepGraph], delta: Int): List[DepGraph] = {
      roots map {
        case SplitRoot(loc, depth) => SplitRoot(loc, depth + delta)
        case x => x
      }
    }
    
    def loopPred(instr: Instruction, roots: List[Either[RangeOperand, IndexRange]], pred: Vector[PredicateInstr]): Either[StackError, IndexRange] = {
      def processOperandBin(op: BinaryOperation with PredicateOp) = {
        val eitherRoots = roots match {
          case Left(hd2) :: Left(hd1) :: tl => Right(Left(BinaryOperand(hd1, op, hd2)) :: tl)
          case _ :: _ :: _ => Left(OperandOpAppliedToRange(instr))
          case _ => Left(PredicateStackUnderflow(instr))
        }
        
        eitherRoots.right flatMap { roots2 => loopPred(instr, roots2, pred.tail) }
      }
      
      def processOperandUn(op: UnaryOperation with PredicateOp) = {
        val eitherRoots = roots match {
          case Left(hd) :: tl => Right(Left(UnaryOperand(op, hd)) :: tl)
          case _ :: _ => Left(OperandOpAppliedToRange(instr))
          case _ => Left(PredicateStackUnderflow(instr))
        }
        
        eitherRoots.right flatMap { roots2 => loopPred(instr, roots2, pred.tail) }
      }
      
      val tail = pred.headOption map {
        case Or => {
          val eitherRoots = roots match {
            case Right(hd2) :: Right(hd1) :: tl => Right(Right(Disjunction(hd1, hd2)) :: tl)
            case _ :: _ :: _ => Left(RangeOpAppliedToOperand(instr))
            case _ => Left(PredicateStackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loopPred(instr, roots2, pred.tail) }
        }
        
        case And => {
          val eitherRoots = roots match {
            case Right(hd2) :: Right(hd1) :: tl => Right(Right(Conjunction(hd1, hd2)) :: tl)
            case _ :: _ :: _ => Left(RangeOpAppliedToOperand(instr))
            case _ => Left(PredicateStackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loopPred(instr, roots2, pred.tail) }
        }
        
        case Comp => {
          val eitherRoots = roots match {
            case Right(hd) :: tl => Right(Right(Complementation(hd)) :: tl)
            case _ :: _ => Left(RangeOpAppliedToOperand(instr))
            case _ => Left(PredicateStackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loopPred(instr, roots2, pred.tail) }
        }
        
        case DerefObject => {
          val eitherRoots = roots match {
            case Left(ValueOperand(hd)) :: tl => Right(Left(PropertyOperand(hd)) :: tl)
            case Left(_) :: _ => Left(DerefObjectAppliedToCompoundOperand(instr))
            case _ :: _ => Left(OperandOpAppliedToRange(instr))
            case _ => Left(PredicateStackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loopPred(instr, roots2, pred.tail) }
        }
        
        case DerefArray => {
          val eitherRoots = roots match {
            case Left(ValueOperand(hd)) :: tl => Right(Left(IndexOperand(hd)) :: tl)
            case Left(_) :: _ => Left(DerefArrayAppliedToCompoundOperand(instr))
            case _ :: _ => Left(OperandOpAppliedToRange(instr))
            case _ => Left(PredicateStackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loopPred(instr, roots2, pred.tail) }
        }
        
        case Range => {
          val eitherRoots = roots match {
            case Left(hd2) :: Left(hd1) :: tl => Right(Right(Contiguous(hd1, hd2)) :: tl)
            case _ :: _ :: _ => Left(OperandOpAppliedToRange(instr))
            case _ => Left(PredicateStackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loopPred(instr, roots2, pred.tail) }
        }
        
        case op: BinaryOperation with PredicateOp => processOperandBin(op)
        case op: UnaryOperation with PredicateOp => processOperandUn(op)
      }
      
      tail getOrElse {
        if (roots.lengthCompare(1) < 0) {
          Left(PredicateStackUnderflow(instr))
        } else if (roots.lengthCompare(1) == 0) {
          roots.head.right.toOption map { Right(_) } getOrElse Left(NonRangePredicateStackAtEnd(instr))
        } else {
          Left(MultiplePredicateStackValuesAtEnd(instr))
        }
      }
    }
    
    def loop(loc: Line, roots: List[DepGraph], splits: List[OpenSplit], stream: Vector[Instruction]): Either[StackError, DepGraph] = {
      def processJoinInstr(instr: JoinInstr) = {
        val eitherRoots = roots match {
          case right :: left :: tl => Right(Join(loc, instr, left, right) :: tl)
          case _ => Left(StackUnderflow(instr))
        }
        
        eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
      }
      
      def processFilter(instr: Instruction, cross: Option[CrossType], depth: Short, pred: Option[Predicate]) = {
        if (depth < 0) {
          Left(NegativePredicateDepth(instr))
        } else {
          val (args, roots2) = roots splitAt (depth + 2)
          
          if (args.lengthCompare(depth + 2) < 0) {
            Left(StackUnderflow(instr))
          } else {
            val (boolean :: target :: predRoots) = args
            val result = pred map { p => loopPred(instr, predRoots map ValueOperand map { Left(_) }, p) }
            val range = result map { _.right map { Some(_) } } getOrElse Right(None)
            
            range.right flatMap { r => loop(loc, Filter(loc, cross, r, target, boolean) :: roots2, splits, stream.tail) }
          }
        }
      }
      
      val tail = stream.headOption map {
        case instr @ Map1(op) => {
          val eitherRoots = roots match {
            case hd :: tl => Right(Operate(loc, op, hd) :: tl)
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case instr: JoinInstr => processJoinInstr(instr)
        
        case instr @ instructions.Reduce(red) => {
          val eitherRoots = roots match {
            case hd :: tl => Right(Reduce(loc, red, hd) :: tl)
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case instructions.Split => {
          roots match {
            case hd :: tl => loop(loc, SplitRoot(loc, 0) :: adjustSplits(tl, 1), OpenSplit(loc, roots) :: splits, stream.tail)
            case _ => Left(StackUnderflow(instructions.Split))
          }
        }
        
        case Merge => {
          val eitherTails = (roots, splits) match {
            case (child :: rootsTail, OpenSplit(loc2, parent :: roots2) :: splitsTail) => {
              if (roots2.tails contains rootsTail)
                Right((Split(loc2, parent, child) :: rootsTail, splitsTail))
              else
                Left(MergeWithUnmatchedTails)
            }
            
            case (_, Nil) => Left(UnmatchedMerge)
            
            case (_ :: Nil, _) => Left(StackUnderflow(Merge))
          }
          
          eitherTails.right flatMap {
            case (roots2, splits2) => loop(loc, adjustSplits(roots2, -1), splits2, stream.tail)
          }
        }
        
        case instr @ FilterMatch(depth, pred) => processFilter(instr, None, depth, pred)
        case instr @ FilterCross(depth, pred) => processFilter(instr, Some(CrossNeutral), depth, pred)
        case instr @ FilterCrossLeft(depth, pred) => processFilter(instr, Some(CrossLeft), depth, pred)
        case instr @ FilterCrossRight(depth, pred) => processFilter(instr, Some(CrossRight), depth, pred)
        
        case Dup => {
          roots match {
            case hd :: tl => loop(loc, hd :: hd :: tl, splits, stream.tail)
            case _ => Left(StackUnderflow(Dup))
          }
        }
        
        case instr @ Swap(depth) => {
          if (depth > 0) {
            if (roots.lengthCompare(depth + 1) < 0) {
              Left(StackUnderflow(instr))
            } else {
              val (span, rest) = roots splitAt (depth + 1)
              val (spanInit, spanTail) = span splitAt depth
              val roots2 = spanTail ::: spanInit.tail ::: (span.head :: rest)
              loop(loc, roots2, splits, stream.tail)
            }
          } else {
            Left(NonPositiveSwapDepth(instr))
          }
        }
        
        case loc2: Line => loop(loc2, roots, splits, stream.tail)
        
        case instr @ instructions.LoadLocal(tpe) => {
          val eitherRoots = roots match {
            case hd :: tl => Right(LoadLocal(loc, None, hd, tpe) :: tl)
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case instr: RootInstr => loop(loc, Root(loc, instr) :: roots, splits, stream.tail)
      }
      
      tail getOrElse {
        if (!splits.isEmpty)
          Left(UnmatchedSplit)
        else if (roots.lengthCompare(1) < 0)
          Left(EmptyStackAtEnd)
        else if (roots.lengthCompare(1) == 0)
          Right(roots.head)
        else
          Left(MultipleStackValuesAtEnd)
      }
    }
    
    def findFirstRoot(line: Option[Line], stream: Vector[Instruction]): Either[StackError, (Root, Vector[Instruction])] = {
      def buildRoot(instr: RootInstr): Either[StackError, (Root, Vector[Instruction])] =
        line map { ln => Right((Root(ln, instr), stream.tail)) } getOrElse Left(UnknownLine)
      
      val back = stream.headOption collect {
        case ln: Line => findFirstRoot(Some(ln), stream.tail)
        
        case i: PushString => buildRoot(i)
        case i: PushNum => buildRoot(i)
        case PushTrue => buildRoot(PushTrue)
        case PushFalse => buildRoot(PushFalse)
        case PushObject => buildRoot(PushObject)
        case PushArray => buildRoot(PushArray)
        
        case instr => Left(StackUnderflow(instr))
      }
      
      back getOrElse Left(EmptyStream)
    }
    
    if (stream.isEmpty) {
      Left(EmptyStream)
    } else {
      findFirstRoot(None, stream).right flatMap { case (root, tail) => loop(root.loc, root :: Nil, Nil, tail) }
    }
  }
  
  private case class OpenSplit(loc: Line, roots: List[DepGraph])
  
  sealed trait DepGraph {
    val loc: Line
  }
  
  object dag {
    case class SplitRoot(loc: Line, depth: Int) extends DepGraph
    case class Root(loc: Line, instr: RootInstr) extends DepGraph
    
    case class LoadLocal(loc: Line, range: Option[IndexRange], parent: DepGraph, tpe: Type) extends DepGraph
    
    case class Operate(loc: Line, op: UnaryOperation, parent: DepGraph) extends DepGraph
    case class Reduce(loc: Line, red: Reduction, parent: DepGraph) extends DepGraph
    
    case class Split(loc: Line, parent: DepGraph, child: DepGraph) extends DepGraph
    
    case class Join(loc: Line, instr: JoinInstr, left: DepGraph, right: DepGraph) extends DepGraph
    case class Filter(loc: Line, cross: Option[CrossType], range: Option[IndexRange], target: DepGraph, boolean: DepGraph) extends DepGraph
    
    
    sealed trait CrossType
    
    case object CrossNeutral extends CrossType
    case object CrossLeft extends CrossType
    case object CrossRight extends CrossType
    
    
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
    
    case class BinaryOperand(left: RangeOperand, op: BinaryOperation with PredicateOp, right: RangeOperand) extends RangeOperand
    case class UnaryOperand(op: UnaryOperation with PredicateOp, child: RangeOperand) extends RangeOperand
  }
  
  
  sealed trait StackError
  
  case object EmptyStream extends StackError
  case class StackUnderflow(instr: Instruction) extends StackError
  case object UnknownLine extends StackError
  
  case object EmptyStackAtEnd extends StackError
  case object MultipleStackValuesAtEnd extends StackError
  
  case class NegativePredicateDepth(instr: Instruction) extends StackError
  case class PredicateStackUnderflow(instr: Instruction) extends StackError
  case class MultiplePredicateStackValuesAtEnd(instr: Instruction) extends StackError
  case class NonRangePredicateStackAtEnd(instr: Instruction) extends StackError
  
  case class OperandOpAppliedToRange(instr: Instruction) extends StackError
  case class RangeOpAppliedToOperand(instr: Instruction) extends StackError
  
  case class DerefObjectAppliedToCompoundOperand(instr: Instruction) extends StackError
  case class DerefArrayAppliedToCompoundOperand(instr: Instruction) extends StackError
  
  case class NonPositiveSwapDepth(instr: Instruction) extends StackError
  
  case object MergeWithUnmatchedTails extends StackError
  case object UnmatchedMerge extends StackError
  case object UnmatchedSplit extends StackError
}
