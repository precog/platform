package com.querio.bytecode

trait DAG extends Instructions {
  import instructions._
  
  def decorate(stream: Vector[Instruction]): Either[StackError, DepGraph] = {
    import dag._
    
    def loopPred(instr: Instruction, roots: List[Either[RangeOperand, IndexRange]], pred: Vector[PredicateInstr]): Either[StackError, IndexRange] = {
      val tail = pred.headOption map {
        case _ => Left(EmptyStream)
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
      def processOpInstr(instr: OpInstr) = {
        val eitherRoots = roots match {
          case hd :: tl => Right(Operate(loc, instr, hd) :: tl)
          case _ => Left(StackUnderflow(instr))
        }
        
        eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
      }
      
      def processJoinInstr(instr: JoinInstr) = {
        val eitherRoots = roots match {
          case hd1 :: hd2 :: tl => Right(Join(loc, instr, hd1, hd2) :: tl)
          case _ => Left(StackUnderflow(instr))
        }
        
        eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
      }
      
      // TODO depth
      def processFilter(instr: Instruction, cross: Boolean, depth: Short, pred: Option[Predicate]) = {
        val (args, roots2) = roots splitAt (depth + 2)
        
        if (args.lengthCompare(depth + 2) < 0) {
          Left(StackUnderflow(instr))
        } else {
          val (target :: boolean :: predRoots) = args
          val result = pred map { p => loopPred(instr, predRoots map ValueOperand map { Left(_) }, p) }
          val range = result map { _.right map { Some(_) } } getOrElse Right(None)
          
          range.right map { r => Filter(loc, cross, r, target, boolean) }
        }
      }
      
      val tail = stream.headOption map {
        case instr: OpInstr => processOpInstr(instr)
        case instr: JoinInstr => processJoinInstr(instr)
        
        case instr @ instructions.Reduce(red) => {
          val eitherRoots = roots match {
            case hd :: tl => Right(Reduce(loc, red, hd) :: tl)
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case instructions.Split =>
          loop(loc, roots, OpenSplit(loc, roots) :: splits, stream.tail)
        
        case Merge => {
          val eitherTails = (roots, splits) match {
            case (child :: (rootsTail @ parent :: tail), OpenSplit(loc2, roots2) :: splitsTail) => {
              if (rootsTail == roots2)
                Right((Split(loc2, parent, child) :: tail, splitsTail))
              else
                Left(MergeWithUnmatchedTails)
            }
            
            case (_ :: Nil, _) => Left(StackUnderflow(Merge))
            
            case (_, Nil) => Left(UnmatchedMerge)
          }
          
          eitherTails.right flatMap {
            case (roots2, splits2) => loop(loc, roots2, splits2, stream.tail)
          }
        }
        
        case instr @ FilterMatch(depth, pred) => processFilter(instr, false, depth, pred)
        case instr @ FilterCross(depth, pred) => processFilter(instr, true, depth, pred)
        
        case Dup => {
          roots match {
            case hd :: tl => loop(loc, hd :: hd :: tl, splits, stream.tail)
            case _ => Left(StackUnderflow(Dup))
          }
        }
        
        case instr @ Swap(depth) => {
          if (depth > 0) {
            if (roots.lengthCompare(depth) < 0) {
              Left(StackUnderflow(instr))
            } else {
              val (span, rest) = roots splitAt depth
              val roots2 = span.last :: (span.foldRight(span.head :: rest) { _ :: _ } tail)
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
        if (roots.lengthCompare(1) < 0)
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
    case class Root(loc: Line, instr: RootInstr) extends DepGraph
    
    case class LoadLocal(loc: Line, range: Option[IndexRange], parent: DepGraph, tpe: Type) extends DepGraph
    
    case class Operate(loc: Line, instr: OpInstr, parent: DepGraph) extends DepGraph
    case class Reduce(loc: Line, red: Reduction, parent: DepGraph) extends DepGraph
    
    case class Split(loc: Line, parent: DepGraph, child: DepGraph) extends DepGraph
    
    case class Join(loc: Line, instr: JoinInstr, left: DepGraph, right: DepGraph) extends DepGraph
    case class Filter(loc: Line, cross: Boolean, range: Option[IndexRange], target: DepGraph, boolean: DepGraph) extends DepGraph
    
    
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
  
  
  sealed trait StackError
  
  case object EmptyStream extends StackError
  case class StackUnderflow(instr: Instruction) extends StackError
  case object UnknownLine extends StackError
  
  case object EmptyStackAtEnd extends StackError
  case object MultipleStackValuesAtEnd extends StackError
  
  case class PredicateStackUnderflow(instr: Instruction) extends StackError
  case class MultiplePredicateStackValuesAtEnd(instr: Instruction) extends StackError
  case class NonRangePredicateStackAtEnd(instr: Instruction) extends StackError
  
  case class NonPositiveSwapDepth(instr: Instruction) extends StackError
  
  case object MergeWithUnmatchedTails extends StackError
  case object UnmatchedMerge extends StackError
}
