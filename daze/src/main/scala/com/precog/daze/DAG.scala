package com.precog
package daze

import bytecode._

import com.precog.util.IdGen
import com.precog.yggdrasil._

import scala.collection.mutable

trait DAG extends Instructions {
  import instructions._
  
  def decorate(stream: Vector[Instruction]): Either[StackError, DepGraph] = {
    import dag._
    
    val adjustMemotable = mutable.Map[(Int, DepGraph), DepGraph]()
    
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
    
    def loop(loc: Line, roots: List[Either[BucketSpec, DepGraph]], splits: List[OpenSplit], stream: Vector[Instruction]): Either[StackError, DepGraph] = {
      def processJoinInstr(instr: JoinInstr) = {
        val eitherRoots = roots match {
          case Right(right) :: Right(left) :: tl => Right(Right(Join(loc, instr, left, right)) :: tl)
          case Left(_) :: _ | _ :: Left(_) :: _ => Left(OperationOnBucket(instr))
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
            val rightArgs = args flatMap { _.right.toOption }
            
            if (rightArgs.lengthCompare(depth + 2) < 0) {
              Left(OperationOnBucket(instr))
            } else {
              val (boolean :: target :: predRoots) = rightArgs
              val result = pred map { p => loopPred(instr, predRoots map ValueOperand map { Left(_) }, p) }
              val range = result map { _.right map { Some(_) } } getOrElse Right(None)
              
              range.right flatMap { r => loop(loc, Right(Filter(loc, cross, r, target, boolean)) :: roots2, splits, stream.tail) }
            }
          }
        }
      }
      
      val tail = stream.headOption map {
        case instr @ Map1(instructions.New) => {
          val eitherRoots = roots match {
            case Right(hd) :: tl => Right(Right(New(loc, hd)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case instr @ Map1(op) => {
          val eitherRoots = roots match {
            case Right(hd) :: tl => Right(Right(Operate(loc, op, hd)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case instr: JoinInstr => processJoinInstr(instr)
        
        case instr @ instructions.Reduce(BuiltInReduction(red)) => {
          val eitherRoots = roots match {
            case Right(hd) :: tl => Right(Right(Reduce(loc, red, hd)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }

        case instr @ instructions.Morph1(BuiltInMorphism(m1)) => {
          val eitherRoots = roots match {
            case Right(hd) :: tl => Right(Right(Morph1(loc, m1, hd)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }

        case instr @ instructions.Morph2(BuiltInMorphism(m2)) => {
          val eitherRoots = roots match {
            case Right(right) :: Right(left) :: tl => Right(Right(Morph2(loc, m2, left, right)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ :: Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case instructions.Distinct => {
          val eitherRoots = roots match {
            case Right(hd) :: tl => Right(Right(Distinct(loc, hd)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instructions.Distinct))
            case _ => Left(StackUnderflow(instructions.Distinct))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case instr @ instructions.Group(id) => {
          val eitherRoots = roots match {
            case Right(target) :: Left(child) :: tl => Right(Left(Group(id, target, child)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case Right(_) :: Right(_) :: _ => Left(BucketOperationOnSets(instr))
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case instr @ MergeBuckets(and) => {
          val const = if (and) IntersectBucketSpec else UnionBucketSpec
          
          val eitherRoots = roots match {
            case Left(right) :: Left(left) :: tl => Right(Left(const(left, right)) :: tl)
            case Right(_) :: _ :: _ => Left(BucketOperationOnSets(instr))
            case _ :: Right(_) :: _ => Left(BucketOperationOnSets(instr))
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case instr @ KeyPart(id) => {
          val eitherRoots = roots match {
            case Right(parent) :: tl => Right(Left(UnfixedSolution(id, parent)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case instructions.Extra => {
          val eitherRoots = roots match {
            case Right(parent) :: tl => Right(Left(Extra(parent)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instructions.Extra))
            case _ => Left(StackUnderflow(instructions.Extra))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case instructions.Split => {
          roots match {
            case Left(spec) :: tl =>
              loop(loc, tl, OpenSplit(loc, spec, tl) :: splits, stream.tail)
            
            case Right(_) :: _ => Left(OperationOnBucket(instructions.Split))
            case _ => Left(StackUnderflow(instructions.Split))
          }
        }
        
        case Merge => {
          val (eitherRoots, splits2) = splits match {
            case (open @ OpenSplit(loc, spec, oldTail)) :: splitsTail => {
              roots match {
                case Right(child) :: tl => {
                  val oldTailSet = Set(oldTail: _*)
                  val newTailSet = Set(tl: _*)
                  
                  if ((oldTailSet & newTailSet).size == newTailSet.size) {
                    val split = Split(loc, spec, child)
                    open.result = split
                    
                    (Right(Right(split) :: tl), splitsTail)
                  } else {
                    (Left(MergeWithUnmatchedTails), splitsTail)
                  }
                }
                
                case _ => (Left(StackUnderflow(Merge)), splitsTail)
              }
            }
            
            case Nil => (Left(UnmatchedMerge), Nil)
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits2, stream.tail) }
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
        
        case Drop => {
          roots match {
            case hd :: tl => loop(loc, tl, splits, stream.tail)
            case _ => Left(StackUnderflow(Drop))
          }
        }
        
        // TODO reenable lines
        case _: Line => loop(loc, roots, splits, stream.tail)
        
        case instr @ instructions.LoadLocal(tpe) => {
          val eitherRoots = roots match {
            case Right(hd) :: tl => Right(Right(LoadLocal(loc, None, hd, tpe)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case PushKey(id) => {
          val openPoss = splits find { open => findGraphWithId(id)(open.spec).isDefined }
          openPoss map { open =>
            loop(loc, Right(SplitParam(loc, id)(open.result)) :: roots, splits, stream.tail)
          } getOrElse Left(UnableToLocateSplitDescribingId(id))
        }
        
        case PushGroup(id) => {
          val openPoss = splits find { open => findGraphWithId(id)(open.spec).isDefined }
          openPoss map { open =>
            val graph = findGraphWithId(id)(open.spec).get
            loop(loc, Right(SplitGroup(loc, id, graph.provenance)(open.result)) :: roots, splits, stream.tail)
          } getOrElse Left(UnableToLocateSplitDescribingId(id))
        }
        
        case instr: RootInstr => loop(loc, Right(Root(loc, instr)) :: roots, splits, stream.tail)
      }
      
      tail getOrElse {
        if (!splits.isEmpty) {
          Left(UnmatchedSplit)
        } else {
          roots match {
            case Right(hd) :: Nil => Right(hd)
            case Left(_) :: Nil => Left(BucketAtEnd)
            case _ :: _ :: _ => Left(MultipleStackValuesAtEnd)
            case Nil => Left(EmptyStackAtEnd)
          }
        }
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
        case PushNull => buildRoot(PushNull)
        case PushObject => buildRoot(PushObject)
        case PushArray => buildRoot(PushArray)
        
        case instr => Left(StackUnderflow(instr))
      }
      
      back getOrElse Left(EmptyStream)
    }
    
    if (stream.isEmpty) {
      Left(EmptyStream)
    } else {
      findFirstRoot(None, stream).right flatMap {
        case (root, tail) => loop(root.loc, Right(root) :: Nil, Nil, tail)
      }
    }
  }
  
  private def findGraphWithId(id: Int)(spec: dag.BucketSpec): Option[DepGraph] = spec match {
    case dag.UnionBucketSpec(left, right) => findGraphWithId(id)(left) orElse findGraphWithId(id)(right)
    case dag.IntersectBucketSpec(left, right) => findGraphWithId(id)(left) orElse findGraphWithId(id)(right)
    case dag.Group(`id`, target, _) => Some(target)
    case dag.Group(_, _, child) => findGraphWithId(id)(child)
    case dag.UnfixedSolution(`id`, target) => Some(target)
    case dag.UnfixedSolution(_, _) => None
    case dag.Extra(_) => None
  }
  
  private class IdentityContainer(private val self: AnyRef) {
    
    override def equals(that: Any) = that match {
      case c: IdentityContainer => c.self eq self
      case _ => false
    }
    
    override def hashCode = System.identityHashCode(self)
    
    override def toString = self.toString
  }
  
  private case class OpenSplit(loc: Line, spec: dag.BucketSpec, oldTail: List[Either[dag.BucketSpec, DepGraph]]) {
    var result: dag.Split = _           // gross!
  }
  
  sealed trait DepGraph {
    val loc: Line
    
    def provenance: Vector[dag.Provenance]
    
    def value: Option[SValue] = None
    
    def isSingleton: Boolean  //true implies that the node is a singleton; false doesn't imply anything 
    
    lazy val memoId = IdGen.nextInt()
    
    def findMemos(parent: dag.Split): Set[Int]
    
    def containsSplitArg: Boolean
  }
  
  object dag {
    //tic variable node
    case class SplitParam(loc: Line, id: Int)(_parent: => Split) extends DepGraph {
      lazy val parent = _parent
      
      val provenance = Vector()
      
      val isSingleton = true
      
      def findMemos(parent: Split) = if (this.parent == parent) Set(memoId) else Set()
      
      val containsSplitArg = true
    }
    
    //grouping node (e.g. foo where foo.a = 'b)
    case class SplitGroup(loc: Line, id: Int, provenance: Vector[Provenance])(_parent: => Split) extends DepGraph {
      lazy val parent = _parent
      
      val isSingleton = false
      
      def findMemos(parent: Split) = if (this.parent == parent) Set(memoId) else Set()
      
      val containsSplitArg = true
    }
    
    case class Root(loc: Line, instr: RootInstr) extends DepGraph {
      lazy val provenance = Vector()
      
      override lazy val value = Some(instr match {
        case PushString(str) => SString(str)
        case PushNum(num) => SDecimal(BigDecimal(num))
        case PushTrue => SBoolean(true)
        case PushFalse => SBoolean(false)
        case PushNull => SNull
        case PushObject => SObject(Map())
        case PushArray => SArray(Vector())
      })
      
      val isSingleton = true
      
      def findMemos(s: Split) = Set()
      
      val containsSplitArg = false
    }
    
    case class New(loc: Line, parent: DepGraph) extends DepGraph {
      lazy val provenance = Vector(DynamicProvenance(IdGen.nextInt()))
      
      override lazy val value = parent.value
      
      lazy val isSingleton = parent.isSingleton
      
      def findMemos(s: Split) = {
        val back = parent.findMemos(s)
        if (back.isEmpty)
          back
        else
          back + memoId
      }
      
      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class Morph1(loc: Line, m: Morphism, parent: DepGraph) extends DepGraph {
      lazy val provenance = Vector(DynamicProvenance(IdGen.nextInt()))
      
      lazy val isSingleton = false
      
      def findMemos(s: Split) = {
        val back = parent.findMemos(s)
        if (back.isEmpty)
          back
        else
          back + memoId
      }
      
      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class Morph2(loc: Line, m: Morphism, left: DepGraph, right: DepGraph) extends DepGraph {
      lazy val provenance = Vector(DynamicProvenance(IdGen.nextInt()))
      
      lazy val isSingleton = false
      
      def findMemos(s: Split) = {
        val back = left.findMemos(s) ++ right.findMemos(s)
        if (back.isEmpty)
          back
        else
          back + memoId
      }
      
      lazy val containsSplitArg = left.containsSplitArg || right.containsSplitArg
    }

    case class Distinct(loc: Line, parent: DepGraph) extends DepGraph {
      lazy val provenance = Vector(DynamicProvenance(IdGen.nextInt()))
      
      lazy val isSingleton = parent.isSingleton
      
      def findMemos(s: Split) = {
        val back = parent.findMemos(s)
        if (back.isEmpty)
          back
        else
          back + memoId
      }
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class LoadLocal(loc: Line, range: Option[IndexRange], parent: DepGraph, tpe: Type) extends DepGraph {
      lazy val provenance = parent match {
        case Root(_, PushString(path)) => Vector(StaticProvenance(path))
        case _ => Vector(DynamicProvenance(IdGen.nextInt()))
      }
      
      val isSingleton = false
      
      def findMemos(s: Split) = {
        val back = parent.findMemos(s)
        if (back.isEmpty)
          back
        else
          back + memoId
      }
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    // TODO propagate AOT value computation
    case class Operate(loc: Line, op: UnaryOperation, parent: DepGraph) extends DepGraph {
      lazy val provenance = parent.provenance
      
      lazy val isSingleton = parent.isSingleton
      
      def findMemos(s: Split) = {
        val back = parent.findMemos(s)
        if (back.isEmpty)
          back
        else
          back + memoId
      }
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class Reduce(loc: Line, red: Reduction, parent: DepGraph) extends DepGraph {
      lazy val provenance = Vector()
      
      val isSingleton = true
      
      def findMemos(s: Split) = {
        val back = parent.findMemos(s)
        if (back.isEmpty)
          back
        else
          back + memoId
      }
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class Split(loc: Line, spec: BucketSpec, child: DepGraph) extends DepGraph {
      lazy val provenance = Vector(DynamicProvenance(IdGen.nextInt()))
      
      lazy val isSingleton = false
      
      lazy val memoIds = Vector(IdGen.nextInt())
      
      def findMemos(s: Split) = {
        def loop(spec: BucketSpec): Set[Int] = spec match {
          case UnionBucketSpec(left, right) =>
            loop(left) ++ loop(right)
          
          case IntersectBucketSpec(left, right) =>
            loop(left) ++ loop(right)
          
          case Group(_, target, child) =>
            target.findMemos(s) ++ loop(child)
          
          case UnfixedSolution(_, target) => target.findMemos(s)
          case Extra(target) => target.findMemos(s)
        }
        
        val back = loop(spec) ++ child.findMemos(s)
        if (back.isEmpty)
          back
        else
          back + memoId
      }
      
      lazy val containsSplitArg = {
        def loop(spec: BucketSpec): Boolean = spec match {
          case UnionBucketSpec(left, right) =>
            loop(left) || loop(right)
          
          case IntersectBucketSpec(left, right) =>
            loop(left) || loop(right)
          
          case Group(_, target, child) =>
            target.containsSplitArg || loop(child)
          
          case UnfixedSolution(_, target) => target.containsSplitArg
          case Extra(target) => target.containsSplitArg
        }
        
        loop(spec)
      }
    }
    
    // TODO propagate AOT value computation
    case class Join(loc: Line, instr: JoinInstr, left: DepGraph, right: DepGraph) extends DepGraph {
      lazy val provenance = instr match {
        case IUnion | IIntersect =>
          Vector(Stream continually DynamicProvenance(IdGen.nextInt()) take left.provenance.length: _*)

        case SetDifference => left.provenance

        case _: Map2CrossRight => right.provenance ++ left.provenance
        case _: Map2Cross | _: Map2CrossLeft => left.provenance ++ right.provenance
        
        case _ => (left.provenance ++ right.provenance).distinct
      }
      
      lazy val isSingleton = left.isSingleton && right.isSingleton
      
      def findMemos(s: Split) = {
        val back = left.findMemos(s) ++ right.findMemos(s)
        if (back.isEmpty)
          back
        else
          back + memoId
      }
      
      lazy val containsSplitArg = left.containsSplitArg || right.containsSplitArg
    }
    
    case class Filter(loc: Line, cross: Option[CrossType], range: Option[IndexRange], target: DepGraph, boolean: DepGraph) extends DepGraph {
      lazy val provenance = cross match {
        case Some(CrossRight) => boolean.provenance ++ target.provenance
        case Some(CrossLeft) | Some(CrossNeutral) => target.provenance ++ boolean.provenance
        case None => (target.provenance ++ boolean.provenance).distinct
      }
      
      lazy val isSingleton = target.isSingleton
      
      def findMemos(s: Split) = {
        val back = target.findMemos(s) ++ boolean.findMemos(s)
        if (back.isEmpty)
          back
        else
          back + memoId
      }
      
      lazy val containsSplitArg = target.containsSplitArg || boolean.containsSplitArg
    }
    
    case class Sort(parent: DepGraph, indexes: Vector[Int]) extends DepGraph {
      val loc = parent.loc
      
      lazy val provenance = {
        val (first, second) = parent.provenance.zipWithIndex partition {
          case (_, i) => indexes contains i
        }
        
        val prefix = first sortWith {
          case ((_, i1), (_, i2)) => indexes.indexOf(i1) < indexes.indexOf(i2)
        }
        
        val (back, _) = (prefix ++ second).unzip
        back
      }
      
      lazy val isSingleton = parent.isSingleton
      
      def findMemos(s: Split) = {
        val back = parent.findMemos(s)
        if (back.isEmpty)
          back
        else
          back + memoId
      }
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class Memoize(parent: DepGraph, priority: Int) extends DepGraph {
      val loc = parent.loc
      
      lazy val provenance = parent.provenance
      lazy val isSingleton = parent.isSingleton
      
      def findMemos(s: Split) = {
        val back = parent.findMemos(s)
        if (back.isEmpty)
          back
        else
          back + memoId
      }
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    
    sealed trait BucketSpec
    
    case class UnionBucketSpec(left: BucketSpec, right: BucketSpec) extends BucketSpec
    case class IntersectBucketSpec(left: BucketSpec, right: BucketSpec) extends BucketSpec
    
    case class Group(id: Int, target: DepGraph, forest: BucketSpec) extends BucketSpec
    
    case class UnfixedSolution(id: Int, solution: DepGraph) extends BucketSpec
    case class Extra(expr: DepGraph) extends BucketSpec
    
    
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
  
    sealed trait Provenance
    
    case class StaticProvenance(path: String) extends Provenance
    case class DynamicProvenance(id: Int) extends Provenance
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
  
  case class OperationOnBucket(instr: Instruction) extends StackError
  case object BucketOperationOnBucket extends StackError
  case class BucketOperationOnSets(instr: Instruction) extends StackError
  case object BucketAtEnd extends StackError
  
  case class UnableToLocateSplitDescribingId(id: Int) extends StackError
}
