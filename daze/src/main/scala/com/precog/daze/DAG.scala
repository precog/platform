package com.precog
package daze

import bytecode._

import blueeyes.json.JNum

import com.precog.util.IdGen
import com.precog.yggdrasil._

import scala.collection.mutable

import scalaz.{NonEmptyList => NEL, _}
import scalaz.Free.Trampoline
import scalaz.std.either._
import scalaz.std.option._
import scalaz.std.list._
import scalaz.syntax.monad._
import scalaz.syntax.semigroup._
import scalaz.syntax.traverse._

import java.math.MathContext

trait DAG extends Instructions with TransSpecModule {
  import instructions._
  
  def decorate(stream: Vector[Instruction]): Either[StackError, DepGraph] = {
    import dag._
    
    val adjustMemotable = mutable.Map[(Int, DepGraph), DepGraph]()
    implicit val M: Traverse[({ type λ[α] = Either[StackError, α] })#λ] with Monad[({ type λ[α] = Either[StackError, α] })#λ] = eitherMonad[StackError]
    
    def loop(loc: Line, roots: List[Either[BucketSpec, DepGraph]], splits: List[OpenSplit], stream: Vector[Instruction]): Trampoline[Either[StackError, DepGraph]] = {
      @inline def continue(f: List[Either[BucketSpec, DepGraph]] => Either[StackError, List[Either[BucketSpec, DepGraph]]]): Trampoline[Either[StackError, DepGraph]] = {
        M.sequence(f(roots).right map { roots2 => loop(loc, roots2, splits, stream.tail) }).map(_.joinRight)
      }

      def processJoinInstr(instr: JoinInstr) = {
        val maybeOpSort = Some(instr) collect {
          case instructions.Map2Match(op) => (op, IdentitySort)
          case instructions.Map2Cross(op) => (op, CrossLeftSort)
          case instructions.Map2CrossLeft(op) => (op, CrossLeftSort)
          case instructions.Map2CrossRight(op) => (op, CrossRightSort)
        }
        
        val eitherRootsOp = maybeOpSort map {
          case (op, joinSort) => 
            continue {
              case Right(right) :: Right(left) :: tl => Right(Right(Join(loc, op, joinSort, left, right)) :: tl)
              case Left(_) :: _ | _ :: Left(_) :: _ => Left(OperationOnBucket(instr))
              case _ => Left(StackUnderflow(instr))
            }
        }
        
        val eitherRootsAbom = Some(instr) collect {
          case instr @ instructions.Assert => 
            continue {
              case Right(child) :: Right(pred) :: tl => Right(Right(Assert(loc, pred, child)) :: tl)
              case Left(_) :: _ | _ :: Left(_) :: _ => Left(OperationOnBucket(instr))
              case _ => Left(StackUnderflow(instr))
            }
          
          case instr @ (instructions.IIntersect | instructions.IUnion) => 
            continue {
              case Right(right) :: Right(left) :: tl => Right(Right(IUI(loc, instr == instructions.IUnion, left, right)) :: tl)
              case Left(_) :: _ | _ :: Left(_) :: _ => Left(OperationOnBucket(instr))
              case _ => Left(StackUnderflow(instr))
            }
          
          case instructions.SetDifference => 
            continue {
              case Right(right) :: Right(left) :: tl => Right(Right(Diff(loc, left, right)) :: tl)
              case Left(_) :: _ | _ :: Left(_) :: _ => Left(OperationOnBucket(instr))
              case _ => Left(StackUnderflow(instr))
            }
        }
        
        eitherRootsOp orElse eitherRootsAbom get      // assertion
      }
      
      def processFilter(instr: Instruction, joinSort: JoinSort): Trampoline[Either[StackError, DepGraph]] = {
        val (args, roots2) = roots splitAt 2
        
        if (args.lengthCompare(2) < 0) {
          Left(StackUnderflow(instr)).point[Trampoline]
        } else {
          val rightArgs = args flatMap { _.right.toOption }
          
          if (rightArgs.lengthCompare(2) < 0) {
            Left(OperationOnBucket(instr)).point[Trampoline]
          } else {
            val (boolean :: target :: predRoots) = rightArgs
            loop(loc, Right(Filter(loc, joinSort, target, boolean)) :: roots2, splits, stream.tail)
          }
        }
      }
      
      val tail: Option[Trampoline[Either[StackError, DepGraph]]] = stream.headOption map {
        case instr @ Map1(instructions.New) => {
          continue {
            case Right(hd) :: tl => Right(Right(New(loc, hd)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
        }
        
        case instr @ Map1(op) => {
          continue {
            case Right(hd) :: tl => Right(Right(Operate(loc, op, hd)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
        }
        
        case instr: JoinInstr => processJoinInstr(instr)

        case instr @ instructions.Morph1(BuiltInMorphism1(m1)) => {
          continue {
            case Right(hd) :: tl => Right(Right(Morph1(loc, m1, hd)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
        }

        case instr @ instructions.Morph2(BuiltInMorphism2(m2)) => {
          continue {
            case Right(right) :: Right(left) :: tl => Right(Right(Morph2(loc, m2, left, right)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ :: Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
        }
        
        case instr @ instructions.Reduce(BuiltInReduction(red)) => {
          continue {
            case Right(hd) :: tl => Right(Right(Reduce(loc, red, hd)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
        }
        
        case instructions.Distinct => {
          continue {
            case Right(hd) :: tl => Right(Right(Distinct(loc, hd)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instructions.Distinct))
            case _ => Left(StackUnderflow(instructions.Distinct))
          }
        }
        
        case instr @ instructions.Group(id) => {
          continue {
            case Right(target) :: Left(child) :: tl => Right(Left(Group(id, target, child)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case Right(_) :: Right(_) :: _ => Left(BucketOperationOnSets(instr))
            case _ => Left(StackUnderflow(instr))
          }
        }
        
        case instr @ MergeBuckets(and) => {
          val const = if (and) IntersectBucketSpec else UnionBucketSpec
          
          continue {
            case Left(right) :: Left(left) :: tl => Right(Left(const(left, right)) :: tl)
            case Right(_) :: _ :: _ => Left(BucketOperationOnSets(instr))
            case _ :: Right(_) :: _ => Left(BucketOperationOnSets(instr))
            case _ => Left(StackUnderflow(instr))
          }
        }
        
        case instr @ KeyPart(id) => {
          continue {
            case Right(parent) :: tl => Right(Left(UnfixedSolution(id, parent)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
        }
        
        case instructions.Extra => {
          continue {
            case Right(parent) :: tl => Right(Left(Extra(parent)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instructions.Extra))
            case _ => Left(StackUnderflow(instructions.Extra))
          }
        }
        
        case instructions.Split => {
          roots match {
            case Left(spec) :: tl =>
              loop(loc, tl, OpenSplit(loc, spec, tl) :: splits, stream.tail)
            
            case Right(_) :: _ => Left(OperationOnBucket(instructions.Split)).point[Trampoline]
            case _ => Left(StackUnderflow(instructions.Split)).point[Trampoline]
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
          
          M.sequence(eitherRoots.right map { roots2 => loop(loc, roots2, splits2, stream.tail) }).map(_.joinRight)
        }
        
        case instr @ FilterMatch => processFilter(instr, IdentitySort)
        case instr @ FilterCross => processFilter(instr, CrossLeftSort)
        case instr @ FilterCrossLeft => processFilter(instr, CrossLeftSort)
        case instr @ FilterCrossRight => processFilter(instr, CrossRightSort)
        
        case Dup => {
          roots match {
            case hd :: tl => loop(loc, hd :: hd :: tl, splits, stream.tail)
            case _ => Left(StackUnderflow(Dup)).point[Trampoline]
          }
        }
        
        case instr @ Swap(depth) => {
          if (depth > 0) {
            if (roots.lengthCompare(depth + 1) < 0) {
              Left(StackUnderflow(instr)).point[Trampoline]
            } else {
              val (span, rest) = roots splitAt (depth + 1)
              val (spanInit, spanTail) = span splitAt depth
              val roots2 = spanTail ::: spanInit.tail ::: (span.head :: rest)
              loop(loc, roots2, splits, stream.tail)
            }
          } else {
            Left(NonPositiveSwapDepth(instr)).point[Trampoline]
          }
        }
        
        case Drop => {
          roots match {
            case hd :: tl => loop(loc, tl, splits, stream.tail)
            case _ => Left(StackUnderflow(Drop)).point[Trampoline]
          }
        }
        
        // TODO reenable lines
        case _: Line => loop(loc, roots, splits, stream.tail)
        
        case instr @ instructions.LoadLocal => {
          continue {
            case Right(hd) :: tl => Right(Right(LoadLocal(loc, hd)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
        }

        case PushUndefined => {
          loop(loc, Right(Undefined(loc)) :: roots, splits, stream.tail)
        }
        
        case PushKey(id) => {
          val openPoss = splits find { open => findGraphWithId(id)(open.spec).isDefined }
          openPoss map { open =>
            loop(loc, Right(SplitParam(loc, id)(open.result)) :: roots, splits, stream.tail)
          } getOrElse Left(UnableToLocateSplitDescribingId(id)).point[Trampoline]
        }
        
        case PushGroup(id) => {
          val openPoss = splits find { open => findGraphWithId(id)(open.spec).isDefined }
          openPoss map { open =>
            val graph = findGraphWithId(id)(open.spec).get
            loop(loc, Right(SplitGroup(loc, id, graph.identities)(open.result)) :: roots, splits, stream.tail)
          } getOrElse Left(UnableToLocateSplitDescribingId(id)).point[Trampoline]
        }

        case instr: RootInstr => {
          val cvalue = instr match {
            case PushString(str) => CString(str)
            
            // get the numeric coersion
            case PushNum(num) =>
              CType.toCValue(JNum(BigDecimal(num, MathContext.UNLIMITED)))
            
            case PushTrue => CBoolean(true)
            case PushFalse => CBoolean(false)
            case PushNull => CNull
            case PushObject => CEmptyObject
            case PushArray => CEmptyArray
          }
          
          loop(loc, Right(Const(loc, cvalue)) :: roots, splits, stream.tail)
        }
      }
      
      tail getOrElse {
        {
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
        }.point[Trampoline]
      }
    }
    
    def findFirstRoot(line: Option[Line], stream: Vector[Instruction]): Either[StackError, (Root, Vector[Instruction])] = {
      def buildConstRoot(instr: RootInstr): Either[StackError, (Root, Vector[Instruction])] = {
        val cvalue = instr match {
          case PushString(str) => CString(str)
          
          // get the numeric coersion
          case PushNum(num) =>
            CType.toCValue(JNum(BigDecimal(num, MathContext.UNLIMITED)))
          
          case PushTrue => CBoolean(true)
          case PushFalse => CBoolean(false)
          case PushNull => CNull
          case PushObject => CEmptyObject
          case PushArray => CEmptyArray
        }
          
        line map { ln => Right((Const(ln, cvalue), stream.tail)) } getOrElse Left(UnknownLine)
      }
      
      val back = stream.headOption collect {
        case ln: Line => findFirstRoot(Some(ln), stream.tail)
        
        case i: PushString => buildConstRoot(i)
        case i: PushNum => buildConstRoot(i)
        case PushTrue => buildConstRoot(PushTrue)
        case PushFalse => buildConstRoot(PushFalse)
        case PushNull => buildConstRoot(PushNull)
        case PushObject => buildConstRoot(PushObject)
        case PushArray => buildConstRoot(PushArray)

        case PushUndefined =>
          line map { ln => Right((Undefined(ln), stream.tail)) } getOrElse Left(UnknownLine)

        case instr => Left(StackUnderflow(instr))
      }
      
      back getOrElse Left(EmptyStream)
    }
    
    if (stream.isEmpty) {
      Left(EmptyStream)
    } else {
      M.sequence(findFirstRoot(None, stream).right map { case (root, tail) => loop(root.loc, Right(root) :: Nil, Nil, tail) }).map(_.joinRight).run
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
  
  private case class OpenSplit(loc: Line, spec: dag.BucketSpec, oldTail: List[Either[dag.BucketSpec, DepGraph]]) {
    var result: dag.Split = _           // gross!
  }

  sealed trait Identities {
    def ++(other: Identities): Identities = (this, other) match {
      case (Identities.Undefined, _) => Identities.Undefined
      case (_, Identities.Undefined) => Identities.Undefined
      case (Identities.Specs(a), Identities.Specs(b)) => Identities.Specs(a ++ b)
    }

    def length: Int
    def distinct: Identities
    def fold[A](identities: Vector[dag.IdentitySpec] => A, undefined: A): A
  }

  object Identities {
    object Specs extends (Vector[dag.IdentitySpec] => Identities.Specs) {
      def empty = Specs(Vector.empty)
    }
    case class Specs(specs: Vector[dag.IdentitySpec]) extends Identities {
      override def length = specs.length
      override def distinct = Specs(specs.distinct)
      override def fold[A](identities: Vector[dag.IdentitySpec] => A, b: A) = identities(specs)
    }
    case object Undefined extends Identities {
      override def length = 0
      override def distinct = Identities.Undefined
      override def fold[A](identities: Vector[dag.IdentitySpec] => A, undefined: A) = undefined
    }
  }

  sealed trait DepGraph {
    val loc: Line
    
    def identities: Identities
    
    def sorting: dag.TableSort
    
    def isSingleton: Boolean  //true implies that the node is a singleton; false doesn't imply anything 
    
    def containsSplitArg: Boolean

    /**
     * NOTE: Does ''not'' work with `Split` rewrites!  Do not attempt!  Do not
     * even ''think'' of attempting!  The badness that follows will be...bewildering.
     */
    def mapDown(body: (DepGraph => DepGraph) => PartialFunction[DepGraph, DepGraph]): DepGraph = {
      val memotable = mutable.Map[DepGraph, DepGraph]()

      def memoized(_splits: => Map[dag.Split, dag.Split])(node: DepGraph): DepGraph = {
        lazy val splits = _splits
        lazy val pf: PartialFunction[DepGraph, DepGraph] = body(memoized(splits))

        def inner(graph: DepGraph): DepGraph = graph match {
          case x if pf isDefinedAt x => pf(x)
          
          case s @ dag.SplitParam(loc, id) => dag.SplitParam(loc, id)(splits(s.parent))

          case s @ dag.SplitGroup(loc, id, identities) => dag.SplitGroup(loc, id, identities)(splits(s.parent))
          
          case dag.Const(_, _) => graph

          case dag.Undefined(_) => graph

          case dag.New(loc, parent) => dag.New(loc, memoized(splits)(parent))
          
          case dag.Morph1(loc, m, parent) => dag.Morph1(loc, m, memoized(splits)(parent))

          case dag.Morph2(loc, m, left, right) => dag.Morph2(loc, m, memoized(splits)(left), memoized(splits)(right))

          case dag.Distinct(loc, parent) => dag.Distinct(loc, memoized(splits)(parent))

          case dag.LoadLocal(loc, parent, jtpe) => dag.LoadLocal(loc, memoized(splits)(parent), jtpe)

          case dag.Operate(loc, op, parent) => dag.Operate(loc, op, memoized(splits)(parent))

          case dag.Reduce(loc, red, parent) => dag.Reduce(loc, red, memoized(splits)(parent))

          case dag.MegaReduce(loc, reds, parent) => dag.MegaReduce(loc, reds, memoized(splits)(parent))
  
          case s @ dag.Split(loc, spec, child) => {
            lazy val splits2 = splits + (s -> result)
            lazy val spec2 = memoizedSpec(spec, splits2)
            lazy val child2 = memoized(splits2)(child)
            lazy val result: dag.Split = dag.Split(loc, spec2, child2)
            result
          }
          
          case dag.Assert(loc, pred, child) => dag.Assert(loc, memoized(splits)(pred), memoized(splits)(child))
          
          case dag.IUI(loc, union, left, right) => dag.IUI(loc, union, memoized(splits)(left), memoized(splits)(right))

          case dag.Diff(loc, left, right) => dag.Diff(loc, memoized(splits)(left), memoized(splits)(right))

          case dag.Join(loc, op, joinSort, left, right) => dag.Join(loc, op, joinSort, memoized(splits)(left), memoized(splits)(right))

          case dag.Filter(loc, joinSort, target, boolean) => dag.Filter(loc, joinSort, memoized(splits)(target), memoized(splits)(boolean))

          case dag.Sort(parent, indexes) => dag.Sort(memoized(splits)(parent), indexes)

          case dag.SortBy(parent, sortField, valueField, id) => dag.SortBy(memoized(splits)(parent), sortField, valueField, id)

          case dag.ReSortBy(parent, id) => dag.ReSortBy(memoized(splits)(parent), id)

          case dag.Memoize(parent, priority) => dag.Memoize(memoized(splits)(parent), priority)
        }

        def memoizedSpec(spec: dag.BucketSpec, splits: => Map[dag.Split, dag.Split]): dag.BucketSpec = spec match {  //TODO generalize?
          case dag.UnionBucketSpec(left, right) =>
            dag.UnionBucketSpec(memoizedSpec(left, splits), memoizedSpec(right, splits))
          
          case dag.IntersectBucketSpec(left, right) =>
            dag.IntersectBucketSpec(memoizedSpec(left, splits), memoizedSpec(right, splits))
          
          case dag.Group(id, target, child) =>
            dag.Group(id, memoized(splits)(target), memoizedSpec(child, splits))
          
          case dag.UnfixedSolution(id, target) =>
            dag.UnfixedSolution(id, memoized(splits)(target))
          
          case dag.Extra(target) =>
            dag.Extra(memoized(splits)(target))
        }
  
        memotable.get(node) getOrElse {
          val result = inner(node)
          memotable += (node -> result)
          result
        }
      }

      memoized(Map())(this)
    }

    def foldDown[Z](enterSplitChild: Boolean)(f0: PartialFunction[DepGraph, Z])(implicit monoid: Monoid[Z]): Z = {
      val f: PartialFunction[DepGraph, Z] = f0.orElse { case _ => monoid.zero }

      def foldThroughSpec(spec: dag.BucketSpec, acc: Z): Z = spec match {
        case dag.UnionBucketSpec(left, right) =>
          foldThroughSpec(right, foldThroughSpec(left, acc))

        case dag.IntersectBucketSpec(left, right) =>
          foldThroughSpec(right, foldThroughSpec(left, acc))

        case dag.Group(_, target, forest) =>
          foldThroughSpec(forest, foldDown0(target, acc |+| f(target)))

        case dag.UnfixedSolution(_, solution) => foldDown0(solution, acc |+| f(solution))
        case dag.Extra(expr) => foldDown0(expr, acc |+| f(expr))
      }

      def foldDown0(node: DepGraph, acc: Z): Z = node match {
        case dag.SplitParam(_, _) => acc

        case dag.SplitGroup(_, _, identities) => acc

        case node @ dag.Const(_, _) => acc

        case dag.Undefined(_) => acc

        case dag.New(_, parent) => foldDown0(parent, acc |+| f(parent))

        case dag.Morph1(_, _, parent) => foldDown0(parent, acc |+| f(parent))

        case dag.Morph2(_, _, left, right) => 
          val acc2 = foldDown0(left, acc |+| f(left))
          foldDown0(right, acc2 |+| f(right))

        case dag.Distinct(_, parent) => foldDown0(parent, acc |+| f(parent))

        case dag.LoadLocal(_, parent, _) => foldDown0(parent, acc |+| f(parent))

        case dag.Operate(_, _, parent) => foldDown0(parent, acc |+| f(parent))

        case node @ dag.Reduce(_, _, parent) => foldDown0(parent, acc |+| f(parent))

        case node @ dag.MegaReduce(_, _, parent) => foldDown0(parent, acc |+| f(parent))

        case dag.Split(_, specs, child) =>
          val specsAcc = foldThroughSpec(specs, acc)
          if (enterSplitChild)
            foldDown0(child, specsAcc |+| f(child))
          else
            specsAcc
          
        case dag.Assert(_, pred, child) =>
          val acc2 = foldDown0(pred, acc |+| f(pred))
          foldDown0(child, acc2 |+| f(child))

        case dag.IUI(_, _, left, right) =>
          val acc2 = foldDown0(left, acc |+| f(left))
          foldDown0(right, acc2 |+| f(right))

        case dag.Diff(_, left, right) =>
          val acc2 = foldDown0(left, acc |+| f(left))
          foldDown0(right, acc2 |+| f(right))

        case dag.Join(_, _, _, left, right) =>
          val acc2 = foldDown0(left, acc |+| f(left))
          foldDown0(right, acc2 |+| f(right))

        case dag.Filter(_, _, target, boolean) =>
          val acc2 = foldDown0(target, acc |+| f(target))
          foldDown0(boolean, acc2 |+| f(boolean))

        case dag.Sort(parent, _) => foldDown0(parent, acc |+| f(parent))

        case dag.SortBy(parent, _, _, _) => foldDown0(parent, acc |+| f(parent))

        case dag.ReSortBy(parent, _) => foldDown0(parent, acc |+| f(parent))

        case dag.Memoize(parent, _) => foldDown0(parent, acc |+| f(parent))
      }

      foldDown0(this, f(this))
    }
  }
  
  object dag {
    sealed trait StagingPoint extends DepGraph
    sealed trait Root extends DepGraph
    
    object ConstString {
      def unapply(graph: DepGraph): Option[String] = graph match {
        case Const(_, CString(str)) => Some(str)
        case _ => None
      }
    }

    object ConstDecimal {
      def unapply(graph: DepGraph): Option[BigDecimal] = graph match {
        case Const(_, CNum(d)) => Some(d)
        case Const(_, CLong(d)) => Some(d)
        case Const(_, CDouble(d)) => Some(d)
        case _ => None
      }
    }
    
    //tic variable node
    case class SplitParam(loc: Line, id: Int)(_parent: => Split) extends DepGraph {
      lazy val parent = _parent
      
      val identities = Identities.Specs.empty
      
      val sorting = IdentitySort
      
      val isSingleton = true
      
      val containsSplitArg = true
    }
    
    //grouping node (e.g. foo where foo.a = 'b)
    case class SplitGroup(loc: Line, id: Int, identities: Identities)(_parent: => Split) extends DepGraph {
      lazy val parent = _parent
      
      val sorting = IdentitySort
      
      val isSingleton = false
      
      val containsSplitArg = true
    }
    
    case class Const(loc: Line, value: CValue) extends DepGraph with Root {
      lazy val identities = Identities.Specs.empty
      
      val sorting = IdentitySort
      
      val isSingleton = true
      
      val containsSplitArg = false
    }

    case class Undefined(loc: Line) extends DepGraph with Root {
      lazy val identities = Identities.Undefined

      val sorting = IdentitySort

      val isSingleton = false

      val containsSplitArg = false
    }
    
    case class New(loc: Line, parent: DepGraph) extends DepGraph {
      lazy val identities = Identities.Specs(Vector(SynthIds(IdGen.nextInt())))
      
      val sorting = IdentitySort
      
      lazy val isSingleton = parent.isSingleton
      
      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class Morph1(loc: Line, mor: Morphism1, parent: DepGraph) extends DepGraph with StagingPoint {
      lazy val identities = {
        if (mor.retainIds) parent.identities
        else Identities.Specs(Vector(SynthIds(IdGen.nextInt())))
      }
      
      val sorting = IdentitySort
      
      lazy val isSingleton = false
      
      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class Morph2(loc: Line, mor: Morphism2, left: DepGraph, right: DepGraph) extends DepGraph with StagingPoint {
      lazy val identities = {
        if (mor.retainIds) sys.error("not implemented yet") //TODO need to retain only the identities that are being used in the match
        else Identities.Specs(Vector(SynthIds(IdGen.nextInt())))
      }
      
      val sorting = IdentitySort
      
      lazy val isSingleton = false
      
      lazy val containsSplitArg = left.containsSplitArg || right.containsSplitArg
    }

    case class Distinct(loc: Line, parent: DepGraph) extends DepGraph with StagingPoint {
      lazy val identities = Identities.Specs(Vector(SynthIds(IdGen.nextInt())))
      
      val sorting = IdentitySort
      
      lazy val isSingleton = parent.isSingleton
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class LoadLocal(loc: Line, parent: DepGraph, jtpe: JType = JType.JUnfixedT) extends DepGraph with StagingPoint {
      lazy val identities = parent match {
        case Const(_, CString(path)) => Identities.Specs(Vector(LoadIds(path)))
        case Morph1(_, expandGlob, Const(_, CString(path))) => Identities.Specs(Vector(LoadIds(path)))
        case _ => Identities.Specs(Vector(SynthIds(IdGen.nextInt())))
      }
      
      val sorting = IdentitySort
      
      val isSingleton = false
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    // TODO propagate AOT value computation
    case class Operate(loc: Line, op: UnaryOperation, parent: DepGraph) extends DepGraph {
      lazy val identities = parent.identities
      
      lazy val sorting = parent.sorting
      
      lazy val isSingleton = parent.isSingleton
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class Reduce(loc: Line, red: Reduction, parent: DepGraph) extends DepGraph with StagingPoint {
      lazy val identities = Identities.Specs.empty
      
      val sorting = IdentitySort
      
      val isSingleton = true
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class MegaReduce(loc: Line, reds: List[(trans.TransSpec1, List[Reduction])], parent: DepGraph) extends DepGraph with StagingPoint {
      lazy val identities = Identities.Specs.empty
      
      val sorting = IdentitySort
      
      val isSingleton = false
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class Split(loc: Line, spec: BucketSpec, child: DepGraph) extends DepGraph with StagingPoint {
      lazy val identities = Identities.Specs(Vector(SynthIds(IdGen.nextInt())))
      
      val sorting = IdentitySort
      
      lazy val isSingleton = false
      
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
    
    case class Assert(loc: Line, pred: DepGraph, child: DepGraph) extends DepGraph {
      lazy val identities = child.identities
      
      val sorting = child.sorting
      
      lazy val isSingleton = child.isSingleton
      
      lazy val containsSplitArg = pred.containsSplitArg || child.containsSplitArg
    }
    
    case class IUI(loc: Line, union: Boolean, left: DepGraph, right: DepGraph) extends DepGraph with StagingPoint {
      lazy val identities = (left.identities, right.identities) match {
        case (Identities.Specs(a), Identities.Specs(b)) => Identities.Specs((a, b).zipped map CoproductIds)
        case _ => Identities.Undefined
      }

      val sorting = IdentitySort    // TODO not correct!
      
      lazy val isSingleton = left.isSingleton && right.isSingleton
      
      lazy val containsSplitArg = left.containsSplitArg || right.containsSplitArg
    }
    
    case class Diff(loc: Line, left: DepGraph, right: DepGraph) extends DepGraph with StagingPoint {
      lazy val identities = left.identities
      
      val sorting = IdentitySort    // TODO not correct!
      
      lazy val isSingleton = left.isSingleton && right.isSingleton
      
      lazy val containsSplitArg = left.containsSplitArg || right.containsSplitArg
    }
    
    // TODO propagate AOT value computation
    case class Join(loc: Line, op: BinaryOperation, joinSort: JoinSort, left: DepGraph, right: DepGraph) extends DepGraph {
      lazy val identities = joinSort match {
        case CrossRightSort => right.identities ++ left.identities
        case CrossLeftSort => left.identities ++ right.identities
        
        case _ => (left.identities ++ right.identities).distinct
      }
      
      lazy val sorting = joinSort match {
        case tbl: TableSort => tbl
        case CrossLeftSort => left.sorting
        case CrossRightSort => right.sorting
      }
      
      lazy val isSingleton = left.isSingleton && right.isSingleton
      
      lazy val containsSplitArg = left.containsSplitArg || right.containsSplitArg
    }
    
    case class Filter(loc: Line, joinSort: JoinSort, target: DepGraph, boolean: DepGraph) extends DepGraph {
      lazy val identities = joinSort match {
        case CrossRightSort => boolean.identities ++ target.identities
        case CrossLeftSort => target.identities ++ boolean.identities
        case _ => (target.identities ++ boolean.identities).distinct
      }
      
      lazy val sorting = joinSort match {
        case tbl: TableSort => tbl
        case CrossLeftSort => target.sorting
        case CrossRightSort => boolean.sorting
      }
      
      lazy val isSingleton = target.isSingleton
      
      lazy val containsSplitArg = target.containsSplitArg || boolean.containsSplitArg
    }
    
    case class Sort(parent: DepGraph, indexes: Vector[Int]) extends DepGraph with StagingPoint {
      val loc = parent.loc
      
      lazy val identities = parent.identities.fold(specs => {
        val (first, second) = specs.zipWithIndex partition {
          case (_, i) => indexes contains i
        }
        
        val prefix = first sortWith {
          case ((_, i1), (_, i2)) => indexes.indexOf(i1) < indexes.indexOf(i2)
        }
        
        val (back, _) = (prefix ++ second).unzip
        Identities.Specs(back)
      }, Identities.Undefined)
      
      val sorting = IdentitySort
      
      lazy val isSingleton = parent.isSingleton
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    /**
     * Evaluator will deref by `sortField` to get the sort ordering and `valueField`
     * to get the actual value set that is being sorted.  Thus, `parent` is
     * assumed to evaluate to a set of objects containing `sortField` and `valueField`.
     * The identity of the sort should be stable between other sorts that are
     * ''logically'' the same.  Thus, if one were to sort set `foo` by `userId`
     * for later joining with set `bar` sorted by `personId`, those two sorts would
     * be semantically very different, but logically identitical and would thus
     * share the same identity.  This is very important to ensure correctness in
     * evaluation of the `Join` node.
     */
    case class SortBy(parent: DepGraph, sortField: String, valueField: String, id: Int) extends DepGraph with StagingPoint {
      val loc = parent.loc

      lazy val identities = parent.identities
      
      val sorting = ValueSort(id)
      
      lazy val isSingleton = parent.isSingleton
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class ReSortBy(parent: DepGraph, id: Int) extends DepGraph with StagingPoint {
      val loc = parent.loc
      
      lazy val identities = parent.identities
      
      val sorting = ValueSort(id)
      
      lazy val isSingleton = parent.isSingleton
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class Memoize(parent: DepGraph, priority: Int) extends DepGraph with StagingPoint {
      val loc = parent.loc
      
      lazy val identities = parent.identities
      lazy val sorting = parent.sorting
      lazy val isSingleton = parent.isSingleton
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    sealed trait BucketSpec
    
    case class UnionBucketSpec(left: BucketSpec, right: BucketSpec) extends BucketSpec
    case class IntersectBucketSpec(left: BucketSpec, right: BucketSpec) extends BucketSpec
    
    case class Group(id: Int, target: DepGraph, forest: BucketSpec) extends BucketSpec
    
    case class UnfixedSolution(id: Int, solution: DepGraph) extends BucketSpec
    case class Extra(expr: DepGraph) extends BucketSpec
    
    
    sealed trait IdentitySpec

    case class LoadIds(path: String) extends IdentitySpec
    case class SynthIds(id: Int) extends IdentitySpec
    case class CoproductIds(left: IdentitySpec, right: IdentitySpec) extends IdentitySpec

    
    sealed trait JoinSort
    sealed trait TableSort extends JoinSort
    
    case object IdentitySort extends TableSort
    case class ValueSort(id: Int) extends TableSort
    case object NullSort extends TableSort
    
    case object CrossLeftSort extends JoinSort
    case object CrossRightSort extends JoinSort
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
