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
      
      def pushBuckets(zipping: Boolean, split: () => Split)(spec: BucketSpec, acc: (List[DepGraph], Int)): (List[DepGraph], Int) = spec match {
        case MergeBucketSpec(left, _, _) => pushBuckets(zipping, split)(left, acc)     // only the evaluator cares
        
        case ZipBucketSpec(left, right) => {
          val acc2 = pushBuckets(true, split)(right, acc) 
          pushBuckets(zipping, split)(left, acc2)
        }
        
        case SingleBucketSpec(target, _) => {
          val (fragment, index) = acc
          val fragment2 = SplitGroup(loc, index, target.provenance)(split()) :: fragment
          
          if (zipping)
            (fragment2, index - 1)
          else
            (SplitParam(loc, index - 1)(split()) :: fragment2, index - 2)
        }
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
        
        case instr @ instructions.Reduce(red) => {
          val eitherRoots = roots match {
            case Right(hd) :: tl => Right(Right(Reduce(loc, red, hd)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }        

        case instr @ instructions.SetReduce(red) => {
          val eitherRoots = roots match {
            case Right(hd) :: tl => Right(Right(SetReduce(loc, red, hd)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case Bucket => {
          val eitherRoots = roots match {
            case Right(right) :: Right(left) :: tl =>
              Right(Left(SingleBucketSpec(left, right)) :: tl)
            
            case Left(_) :: _ | _ :: Left(_) :: _ =>
              Left(BucketOperationOnBucket)
            
            case _ =>
              Left(StackUnderflow(Bucket))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case instr @ MergeBuckets(and) => {
          val eitherRoots = roots match {
            case Left(right) :: Left(left) :: tl =>
              Right(Left(MergeBucketSpec(left, right, and)) :: tl)
            
            case Right(_) :: _ | _ :: Right(_) :: _ =>
              Left(BucketOperationOnSets(instr))
            
            case _ =>
              Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case ZipBuckets => {
          val eitherRoots = roots match {
            case Left(right) :: Left(left) :: tl =>
              Right(Left(ZipBucketSpec(left, right)) :: tl)
            
            case Right(_) :: _ | _ :: Right(_) :: _ =>
              Left(BucketOperationOnSets(ZipBuckets))
            
            case _ =>
              Left(StackUnderflow(ZipBuckets))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
        }
        
        case instr @ instructions.Split(n, depth) => {
          val eitherBuckets = roots take n
          
          if ((eitherBuckets lengthCompare n) != 0) {
            Left(StackUnderflow(instr))
          } else {
            // reverse needed to match intended semantics
            val buckets = Vector(roots flatMap { _.left.toOption }: _*).reverse
            
            if ((buckets lengthCompare n) != 0) {
              Left(BucketOperationOnSets(instr))
            } else {
              val oldTail = roots drop n
              val open = OpenSplit(loc, buckets, oldTail)
              val (fragment, _) = buckets.foldRight((List[DepGraph](), depth - 1))(pushBuckets(false, () => open.result))
              val roots2 = (fragment map { Right(_) }) ::: oldTail
              
              loop(loc, roots2, open :: splits, stream.tail)
            }
          }
        }
        
        case Merge => {
          val (eitherRoots, splits2) = splits match {
            case (open @ OpenSplit(loc, specs, oldTail)) :: splitsTail => {
              roots match {
                case Right(child) :: tl => {
                  val oldTailSet = Set(oldTail: _*)
                  val newTailSet = Set(tl: _*)
                  
                  if ((oldTailSet & newTailSet).size == newTailSet.size) {
                    val split = Split(loc, specs, child)
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
        
        case loc2: Line => loop(loc2, roots, splits, stream.tail)
        
        case instr @ instructions.LoadLocal(tpe) => {
          val eitherRoots = roots match {
            case Right(hd) :: tl => Right(Right(LoadLocal(loc, None, hd, tpe)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
          
          eitherRoots.right flatMap { roots2 => loop(loc, roots2, splits, stream.tail) }
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
  
  private class IdentityContainer(private val self: AnyRef) {
    
    override def equals(that: Any) = that match {
      case c: IdentityContainer => c.self eq self
      case _ => false
    }
    
    override def hashCode = System.identityHashCode(self)
    
    override def toString = self.toString
  }
  
  private case class OpenSplit(loc: Line, specs: Vector[dag.BucketSpec], oldTail: List[Either[dag.BucketSpec, DepGraph]]) {
    var result: dag.Split = _           // gross!
  }
  
  sealed trait DepGraph {
    val loc: Line
    
    def provenance: Vector[dag.Provenance]
    
    def value: Option[SValue] = None
    
    def isSingleton: Boolean
    
    def findMemos: Set[dag.Memoize]
  }
  
  object dag {
    case class SplitParam(loc: Line, index: Int)(_parent: => Split) extends DepGraph {
      lazy val parent = _parent
      
      val provenance = Vector()
      
      val isSingleton = true
      
      val findMemos = Set[Memoize]()
    }
    
    case class SplitGroup(loc: Line, index: Int, provenance: Vector[Provenance])(_parent: => Split) extends DepGraph {
      lazy val parent = _parent
      
      val isSingleton = false
      
      val findMemos = Set[Memoize]()
    }
    
    case class Root(loc: Line, instr: RootInstr) extends DepGraph {
      lazy val provenance = Vector()
      
      override lazy val value = Some(instr match {
        case PushString(str) => SString(str)
        case PushNum(num) => SDecimal(BigDecimal(num))
        case PushTrue => SBoolean(true)
        case PushFalse => SBoolean(false)
        case PushObject => SObject(Map())
        case PushArray => SArray(Vector())
      })
      
      val isSingleton = true
      
      val findMemos = Set[Memoize]()
    }
    
    case class New(loc: Line, parent: DepGraph) extends DepGraph {
      lazy val provenance = Vector(DynamicProvenance(IdGen.nextInt()))
      
      override lazy val value = parent.value
      
      lazy val isSingleton = parent.isSingleton
      
      lazy val findMemos = parent.findMemos
    }    

    case class SetReduce(loc: Line, red: SetReduction, parent: DepGraph) extends DepGraph {
      lazy val provenance = Vector(DynamicProvenance(IdGen.nextInt()))
      
      lazy val isSingleton = parent.isSingleton
      
      lazy val findMemos = parent.findMemos
    }
    
    case class LoadLocal(loc: Line, range: Option[IndexRange], parent: DepGraph, tpe: Type) extends DepGraph {
      lazy val provenance = parent match {
        case Root(_, PushString(path)) => Vector(StaticProvenance(path))
        case _ => Vector(DynamicProvenance(IdGen.nextInt()))
      }
      
      val isSingleton = false
      
      lazy val findMemos = parent.findMemos
    }
    
    // TODO propagate AOT value computation
    case class Operate(loc: Line, op: UnaryOperation, parent: DepGraph) extends DepGraph {
      lazy val provenance = parent.provenance
      
      lazy val isSingleton = parent.isSingleton
      
      lazy val findMemos = parent.findMemos
    }
    
    case class Reduce(loc: Line, red: Reduction, parent: DepGraph) extends DepGraph {
      lazy val provenance = Vector()
      
      val isSingleton = true
      
      lazy val findMemos = parent.findMemos
    }
    
    case class Split(loc: Line, specs: Vector[BucketSpec], child: DepGraph) extends DepGraph {
      lazy val provenance = Vector(DynamicProvenance(IdGen.nextInt()))
      
      lazy val isSingleton = sys.error("todo")
      
      lazy val findMemos = sys.error("todo")
    }
    
    // TODO propagate AOT value computation
    case class Join(loc: Line, instr: JoinInstr, left: DepGraph, right: DepGraph) extends DepGraph {
      lazy val provenance = instr match {
        case IUnion | IIntersect => {
          val size = math.max(left.provenance.length, right.provenance.length)
          (0 until size).foldLeft(Vector.empty[dag.Provenance]) { case (acc, _) => acc :+ DynamicProvenance(IdGen.nextInt()) } 
        }
        case _: Map2CrossRight => right.provenance ++ left.provenance
        case _: Map2Cross | _: Map2CrossLeft => left.provenance ++ right.provenance
        
        case _ => (left.provenance ++ right.provenance).distinct
      }
      
      lazy val isSingleton = left.isSingleton && right.isSingleton
      
      lazy val findMemos = left.findMemos ++ right.findMemos
    }
    
    case class Filter(loc: Line, cross: Option[CrossType], range: Option[IndexRange], target: DepGraph, boolean: DepGraph) extends DepGraph {
      lazy val provenance = cross match {
        case Some(CrossRight) => boolean.provenance ++ target.provenance
        case Some(CrossLeft) | Some(CrossNeutral) => target.provenance ++ boolean.provenance
        case None => (target.provenance ++ boolean.provenance).distinct
      }
      
      lazy val isSingleton = target.isSingleton
      
      lazy val findMemos = target.findMemos ++ boolean.findMemos
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
      
      lazy val memoId = IdGen.nextInt()
      
      lazy val findMemos = parent.findMemos
    }
    
    case class Memoize(parent: DepGraph, priority: Int) extends DepGraph {
      val loc = parent.loc
      
      lazy val provenance = parent.provenance
      lazy val isSingleton = parent.isSingleton
      
      lazy val memoId = IdGen.nextInt()
      
      lazy val findMemos = parent.findMemos + this
    }
    
    
    sealed trait BucketSpec
    
    case class MergeBucketSpec(left: BucketSpec, right: BucketSpec, and: Boolean) extends BucketSpec
    case class ZipBucketSpec(left: BucketSpec, right: BucketSpec) extends BucketSpec
    case class SingleBucketSpec(target: DepGraph, solution: DepGraph) extends BucketSpec
    
    
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
}
