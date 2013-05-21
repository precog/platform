package com.precog
package daze

import bytecode._

import blueeyes.json.JNum

import com.precog.common._
import com.precog.util.{IdGen, Identifier}
import com.precog.yggdrasil._

import scala.collection.mutable

import scalaz.{NonEmptyList => NEL, _}
import scalaz.Free.Trampoline
import scalaz.std.either._
import scalaz.std.function._
import scalaz.std.option._
import scalaz.std.list._
import scalaz.syntax.monad._
import scalaz.syntax.semigroup._
import scalaz.syntax.traverse._

import java.math.MathContext

trait DAG extends Instructions {
  type TS1

  import instructions._
  import library._
  
  def decorate(stream: Vector[Instruction]): Either[StackError, DepGraph] = {
    import dag._
    
    val adjustMemotable = mutable.Map[(Int, DepGraph), DepGraph]()
    implicit val M: Traverse[({ type λ[α] = Either[StackError, α] })#λ] with Monad[({ type λ[α] = Either[StackError, α] })#λ] = eitherMonad[StackError]
    
    def loop(loc: Line, roots: List[Either[BucketSpec, DepGraph]], splits: List[OpenSplit], stream: Vector[Instruction]): Trampoline[Either[StackError, DepGraph]] = {
      @inline def continue(f: List[Either[BucketSpec, DepGraph]] => Either[StackError, List[Either[BucketSpec, DepGraph]]]): Trampoline[Either[StackError, DepGraph]] = {
        Free.suspend(M.sequence(f(roots).right map { roots2 => loop(loc, roots2, splits, stream.tail) }).map(_.joinRight))
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
              case Right(right) :: Right(left) :: tl => Right(Right(Join(op, joinSort, left, right)(loc)) :: tl)
              case Left(_) :: _ | _ :: Left(_) :: _ => Left(OperationOnBucket(instr))
              case _ => Left(StackUnderflow(instr))
            }
        }
        
        val eitherRootsAbom = Some(instr) collect {
          case instr @ instructions.Observe => 
            continue {
              case Right(samples) :: Right(data) :: tl => Right(Right(Observe(data, samples)(loc)) :: tl)
              case Left(_) :: _ | _ :: Left(_) :: _ => Left(OperationOnBucket(instr))
              case _ => Left(StackUnderflow(instr))
            }
          
          case instr @ instructions.Assert => 
            continue {
              case Right(child) :: Right(pred) :: tl => Right(Right(Assert(pred, child)(loc)) :: tl)
              case Left(_) :: _ | _ :: Left(_) :: _ => Left(OperationOnBucket(instr))
              case _ => Left(StackUnderflow(instr))
            }
          
          case instr @ (instructions.IIntersect | instructions.IUnion) => 
            continue {
              case Right(right) :: Right(left) :: tl => Right(Right(IUI(instr == instructions.IUnion, left, right)(loc)) :: tl)
              case Left(_) :: _ | _ :: Left(_) :: _ => Left(OperationOnBucket(instr))
              case _ => Left(StackUnderflow(instr))
            }
          
          case instructions.SetDifference => 
            continue {
              case Right(right) :: Right(left) :: tl => Right(Right(Diff(left, right)(loc)) :: tl)
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
            loop(loc, Right(Filter(joinSort, target, boolean)(loc)) :: roots2, splits, stream.tail)
          }
        }
      }
      
      val tail: Option[Trampoline[Either[StackError, DepGraph]]] = stream.headOption map {
        case instr @ Map1(instructions.New) => {
          continue {
            case Right(hd) :: tl => Right(Right(New(hd)(loc)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
        }
        
        case instr @ Map1(op) => {
          continue {
            case Right(hd) :: tl => Right(Right(Operate(op, hd)(loc)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
        }
        
        case instr: JoinInstr => processJoinInstr(instr)

        case instr @ instructions.Morph1(BuiltInMorphism1(m1)) => {
          continue {
            case Right(hd) :: tl => Right(Right(Morph1(m1, hd)(loc)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
        }

        case instr @ instructions.Morph2(BuiltInMorphism2(m2)) => {
          continue {
            case Right(right) :: Right(left) :: tl => Right(Right(Morph2(m2, left, right)(loc)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ :: Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
        }
        
        case instr @ instructions.Reduce(BuiltInReduction(red)) => {
          continue {
            case Right(hd) :: tl => Right(Right(Reduce(red, hd)(loc)) :: tl)
            case Left(_) :: _ => Left(OperationOnBucket(instr))
            case _ => Left(StackUnderflow(instr))
          }
        }
        
        case instructions.Distinct => {
          continue {
            case Right(hd) :: tl => Right(Right(Distinct(hd)(loc)) :: tl)
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
              loop(loc, tl, OpenSplit(loc, spec, tl, new Identifier) :: splits, stream.tail)
            
            case Right(_) :: _ => Left(OperationOnBucket(instructions.Split)).point[Trampoline]
            case _ => Left(StackUnderflow(instructions.Split)).point[Trampoline]
          }
        }
        
        case Merge => {
          val (eitherRoots, splits2) = splits match {
            case (open @ OpenSplit(loc, spec, oldTail, id)) :: splitsTail => {
              roots match {
                case Right(child) :: tl => {
                  val oldTailSet = Set(oldTail: _*)
                  val newTailSet = Set(tl: _*)
                  
                  if ((oldTailSet & newTailSet).size == newTailSet.size) {
                    val split = Split(spec, child, id)(loc)
                    
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
        
        case loc: Line => loop(loc, roots, splits, stream.tail)
        
        case instr @ instructions.LoadLocal => {
          continue {
            case Right(hd) :: tl => Right(Right(LoadLocal(hd)(loc)) :: tl)
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
            loop(loc, Right(SplitParam(id, open.id)(loc)) :: roots, splits, stream.tail)
          } getOrElse Left(UnableToLocateSplitDescribingId(id)).point[Trampoline]
        }
        
        case PushGroup(id) => {
          val openPoss = splits find { open => findGraphWithId(id)(open.spec).isDefined }
          openPoss map { open =>
            val graph = findGraphWithId(id)(open.spec).get
            loop(loc, Right(SplitGroup(id, graph.identities, open.id)(loc)) :: roots, splits, stream.tail)
          } getOrElse Left(UnableToLocateSplitDescribingId(id)).point[Trampoline]
        }

        case instr: RootInstr => {
          val rvalue = instr match {
            case PushString(str) => CString(str)
            
            // get the numeric coersion
            case PushNum(num) =>
              CType.toCValue(JNum(BigDecimal(num, MathContext.UNLIMITED)))
            
            case PushTrue => CBoolean(true)
            case PushFalse => CBoolean(false)
            case PushNull => CNull
            case PushObject => RObject.empty
            case PushArray => RArray.empty
          }
          
          loop(loc, Right(Const(rvalue)(loc)) :: roots, splits, stream.tail)
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
        val rvalue = instr match {
          case PushString(str) => CString(str)
          
          // get the numeric coersion
          case PushNum(num) =>
            CType.toCValue(JNum(BigDecimal(num, MathContext.UNLIMITED)))

          case PushTrue => CBoolean(true)
          case PushFalse => CBoolean(false)
          case PushNull => CNull
          case PushObject => RObject.empty
          case PushArray => RArray.empty
        }
          
        line map { ln => Right((Const(rvalue)(ln), stream.tail)) } getOrElse Left(UnknownLine)
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
  
  private case class OpenSplit(loc: Line, spec: dag.BucketSpec, oldTail: List[Either[dag.BucketSpec, DepGraph]], id: Identifier)

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
      override def distinct = Specs(specs map { _.canonicalize } distinct)
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

    def mapDown(body: (DepGraph => DepGraph) => PartialFunction[DepGraph, DepGraph]): DepGraph = {
      val memotable = mutable.Map[DepGraph, DepGraph]()

      def memoized(node: DepGraph): DepGraph = {
        lazy val pf: PartialFunction[DepGraph, DepGraph] = body(memoized)

        def inner(graph: DepGraph): DepGraph = graph match {
          case x if pf isDefinedAt x => pf(x)
          
          // not using extractors due to bug
          case s: dag.SplitParam =>
            dag.SplitParam(s.id, s.parentId)(s.loc)

          // not using extractors due to bug
          case s: dag.SplitGroup =>
            dag.SplitGroup(s.id, s.identities, s.parentId)(s.loc)
          
          case dag.Const(_) => graph

          case dag.Undefined() => graph

          case graph @ dag.New(parent) => dag.New(memoized(parent))(graph.loc)
          
          case graph @ dag.Morph1(m, parent) => dag.Morph1(m, memoized(parent))(graph.loc)

          case graph @ dag.Morph2(m, left, right) => dag.Morph2(m, memoized(left), memoized(right))(graph.loc)

          case graph @ dag.Distinct(parent) => dag.Distinct(memoized(parent))(graph.loc)

          case graph @ dag.LoadLocal(parent, jtpe) => dag.LoadLocal(memoized(parent), jtpe)(graph.loc)

          case graph @ dag.Operate(op, parent) => dag.Operate(op, memoized(parent))(graph.loc)

          case graph @ dag.Reduce(red, parent) => dag.Reduce(red, memoized(parent))(graph.loc)

          case dag.MegaReduce(reds, parent) => dag.MegaReduce(reds, memoized(parent))
  
          case s @ dag.Split(spec, child, id) => {
            val spec2 = memoizedSpec(spec)
            val child2 = memoized(child)
            dag.Split(spec2, child2, id)(s.loc)
          }
            
          case graph @ dag.Assert(pred, child) => dag.Assert(memoized(pred), memoized(child))(graph.loc)
          
          case graph @ dag.Cond(pred, left, leftJoin, right, rightJoin) =>
            dag.Cond(memoized(pred), memoized(left), leftJoin, memoized(right), rightJoin)(graph.loc)

          case graph @ dag.Observe(data, samples) => dag.Observe(memoized(data), memoized(samples))(graph.loc)

          case graph @ dag.IUI(union, left, right) => dag.IUI(union, memoized(left), memoized(right))(graph.loc)

          case graph @ dag.Diff(left, right) => dag.Diff(memoized(left), memoized(right))(graph.loc)

          case graph @ dag.Join(op, joinSort, left, right) => dag.Join(op, joinSort, memoized(left), memoized(right))(graph.loc)

          case graph @ dag.Filter(joinSort, target, boolean) => dag.Filter(joinSort, memoized(target), memoized(boolean))(graph.loc)

          case dag.Sort(parent, indexes) => dag.Sort(memoized(parent), indexes)

          case dag.SortBy(parent, sortField, valueField, id) => dag.SortBy(memoized(parent), sortField, valueField, id)

          case dag.ReSortBy(parent, id) => dag.ReSortBy(memoized(parent), id)

          case dag.Memoize(parent, priority) => dag.Memoize(memoized(parent), priority)
        }

        def memoizedSpec(spec: dag.BucketSpec): dag.BucketSpec = spec match {  //TODO generalize?
          case dag.UnionBucketSpec(left, right) =>
            dag.UnionBucketSpec(memoizedSpec(left), memoizedSpec(right))
          
          case dag.IntersectBucketSpec(left, right) =>
            dag.IntersectBucketSpec(memoizedSpec(left), memoizedSpec(right))
          
          case dag.Group(id, target, child) =>
            dag.Group(id, memoized(target), memoizedSpec(child))
          
          case dag.UnfixedSolution(id, target) =>
            dag.UnfixedSolution(id, memoized(target))
          
          case dag.Extra(target) =>
            dag.Extra(memoized(target))
        }
  
        memotable.get(node) getOrElse {
          val result = inner(node)
          memotable += (node -> result)
          result
        }
      }

      memoized(this)
    }
    
    trait ScopeUpdate[S] {
      def update(node: DepGraph): Option[S] = None
      def update(spec: dag.BucketSpec): Option[S] = None
    }
    object ScopeUpdate {
      def scopeUpdate[S : ScopeUpdate] = implicitly[ScopeUpdate[S]]
      
      implicit def depGraphScopeUpdate = new ScopeUpdate[DepGraph] {
        override def update(node: DepGraph) = Some(node) 
      }
      implicit def bucketSpecScopeUpdate = new ScopeUpdate[dag.BucketSpec] {
        override def update(spec: dag.BucketSpec) = Some(spec)
      }
    }
    
    trait EditUpdate[E] {
      def edit[T](inScope: Boolean, from: DepGraph, edit: (E, E), replace: DepGraph => T, retain: DepGraph => T): T
      def edit[T](inScope: Boolean, from: dag.BucketSpec, edit: (E, E), replace: dag.BucketSpec => T, retain: dag.BucketSpec => T): T
      def bimap[T](e: E)(fg: DepGraph => T, fs: dag.BucketSpec => T): T
    }
    object EditUpdate {
      def editUpdate[E : EditUpdate] = implicitly[EditUpdate[E]]
      
      implicit def depGraphEditUpdate = new EditUpdate[DepGraph] {
        def edit[T](inScope: Boolean, from: DepGraph, edit: (DepGraph, DepGraph), replace: DepGraph => T, retain: DepGraph => T): T =
          if(inScope && from == edit._1) replace(edit._2) else retain(from) 
        def edit[T](inScope: Boolean, from: dag.BucketSpec, edit: (DepGraph, DepGraph), replace: dag.BucketSpec => T, retain: dag.BucketSpec => T): T = retain(from)
        
        def bimap[T](e: DepGraph)(fg: DepGraph => T, fs: dag.BucketSpec => T): T = fg(e)
      }
      implicit def bucketSpecEditUpdate = new EditUpdate[dag.BucketSpec] {
        def edit[T](inScope: Boolean, from: DepGraph, edit: (dag.BucketSpec, dag.BucketSpec), replace: DepGraph => T, retain: DepGraph => T): T = retain(from)
        def edit[T](inScope: Boolean, from: dag.BucketSpec, edit: (dag.BucketSpec, dag.BucketSpec), replace: dag.BucketSpec => T, retain: dag.BucketSpec => T): T =
          if(inScope && from == edit._1) replace(edit._2) else retain(from)
          
        def bimap[T](e: dag.BucketSpec)(fg: DepGraph => T, fs: dag.BucketSpec => T): T = fs(e)
      }
    }

    /**
     * Performs a scoped node substitution on this DepGraph.
     * 
     * The substitution is represented as a pair from -> to both of type E. The replacement is performed everywhere in
     * the DAG below the scope node of type S. Both S and E are constained to be one of DepGraph or BucketSpec by
     * the availability of instances of the type classes EditUpdate[E] and ScopeUpdate[S] as defined above.
     * 
     * @return A pair of the whole rewritten DAG and the rewritten scope node.
     */  
    
    def substituteDown[S: ScopeUpdate, E: EditUpdate](scope: S, edit: (E, E)): (DepGraph, S) = {
      import ScopeUpdate._
      import EditUpdate._
      
      case class SubstitutionState(inScope: Boolean, rewrittenScope: Option[S])
      
      val monadState = StateT.stateMonad[SubstitutionState]
      val init = SubstitutionState(this == scope, None)
      
      val memotable = mutable.Map.empty[DepGraph, State[SubstitutionState, DepGraph]]
      
      def memoized(node: DepGraph): State[SubstitutionState, DepGraph] = {

        def inner(graph: DepGraph): State[SubstitutionState, DepGraph] = {
          
          val inScopeM =
            for {
              state <- monadState.gets(identity)
              inScope = state.inScope || graph == scope
              _ <- monadState.modify(_.copy(inScope = inScope))
            } yield inScope

          val rewritten =
            inScopeM.flatMap { inScope =>
              editUpdate[E].edit(inScope, graph, edit, (rep: DepGraph) => for { state <- monadState.gets(identity) } yield rep,
                (_: DepGraph) match {
                  // not using extractors due to bug
                  case s: dag.SplitParam =>
                    for { state <- monadState.gets(identity) } yield dag.SplitParam(s.id, s.parentId)(s.loc)
        
                  // not using extractors due to bug
                  case s: dag.SplitGroup =>
                    for { state <- monadState.gets(identity) } yield dag.SplitGroup(s.id, s.identities, s.parentId)(s.loc)
                  
                  case graph @ dag.Const(_) =>
                    for { _ <- monadState.gets(identity) } yield graph
        
                  case graph @ dag.Undefined() =>
                    for { _ <- monadState.gets(identity) } yield graph
        
                  case graph @ dag.New(parent) =>
                    for { newParent <- memoized(parent) } yield dag.New(newParent)(graph.loc)
                  
                  case graph @ dag.Morph1(m, parent) =>
                    for { newParent <- memoized(parent) } yield dag.Morph1(m, newParent)(graph.loc)
        
                  case graph @ dag.Morph2(m, left, right) =>
                    for {
                      newLeft <- memoized(left)
                      newRight <- memoized(right)
                    } yield dag.Morph2(m, newLeft, newRight)(graph.loc)
        
                  case graph @ dag.Distinct(parent) =>
                    for { newParent <- memoized(parent) } yield dag.Distinct(newParent)(graph.loc)
        
                  case graph @ dag.LoadLocal(parent, jtpe) =>
                    for { newParent <- memoized(parent) } yield dag.LoadLocal(newParent, jtpe)(graph.loc)
        
                  case graph @ dag.Operate(op, parent) =>
                    for { newParent <- memoized(parent) } yield dag.Operate(op, newParent)(graph.loc)
        
                  case graph @ dag.Reduce(red, parent) =>
                    for { newParent <- memoized(parent) } yield dag.Reduce(red, newParent)(graph.loc)
        
                  case dag.MegaReduce(reds, parent) =>
                    for { newParent <- memoized(parent) } yield dag.MegaReduce(reds, newParent)
          
                  case s @ dag.Split(spec, child, id) => {
                    for {
                      newSpec <- memoizedSpec(spec)
                      newChild <- memoized(child)
                    } yield dag.Split(newSpec, newChild, id)(s.loc)
                  }
                  
                  case graph @ dag.Assert(pred, child) => 
                    for {
                      newPred <- memoized(pred)
                      newChild <- memoized(child)
                    } yield dag.Assert(newPred, newChild)(graph.loc)
                  
                  case graph @ dag.Observe(data, samples) => 
                    for {
                      newData <- memoized(data)
                      newSamples <- memoized(samples)
                    } yield dag.Observe(newData, newSamples)(graph.loc)
                  
                  case graph @ dag.IUI(union, left, right) =>
                    for {
                      newLeft <- memoized(left)
                      newRight <- memoized(right)
                    } yield dag.IUI(union, newLeft, newRight)(graph.loc)
        
                  case graph @ dag.Diff(left, right) =>
                    for {
                      newLeft <- memoized(left)
                      newRight <- memoized(right)
                    } yield dag.Diff(newLeft, newRight)(graph.loc)
        
                  case graph @ dag.Join(op, joinSort, left, right) =>
                    for {
                      newLeft <- memoized(left)
                      newRight <- memoized(right)
                    } yield dag.Join(op, joinSort, newLeft, newRight)(graph.loc)
        
                  case graph @ dag.Filter(joinSort, target, boolean) =>
                    for {
                      newTarget <- memoized(target)
                      newBoolean <- memoized(boolean)
                    } yield dag.Filter(joinSort, newTarget, newBoolean)(graph.loc)
        
                  case dag.Sort(parent, indexes) =>
                    for { newParent <- memoized(parent) } yield dag.Sort(newParent, indexes)
        
                  case dag.SortBy(parent, sortField, valueField, id) =>
                    for { newParent <- memoized(parent) } yield dag.SortBy(newParent, sortField, valueField, id)
        
                  case dag.ReSortBy(parent, id) =>
                    for { newParent <- memoized(parent) } yield dag.ReSortBy(newParent, id)
        
                  case dag.Memoize(parent, priority) =>
                    for { newParent <- memoized(parent) } yield dag.Memoize(newParent, priority)
                }
              )
            }
          
          if(graph != scope)
            rewritten
          else
            for {
              node <- rewritten
              _ <- monadState.modify(_.copy(rewrittenScope = scopeUpdate[S].update(node)))
            } yield node
        }
        
        memotable.get(node) getOrElse {
          val result = inner(node)
          memotable += (node -> result)
          result
        }
      }

      def memoizedSpec(spec: dag.BucketSpec): State[SubstitutionState, dag.BucketSpec] = {
        val inScopeM =
          for {
            state <- monadState.gets(identity)
            inScope = state.inScope || spec == scope
            _ <- monadState.modify(_.copy(inScope = inScope))
          } yield inScope
          
        val rewritten =
          inScopeM.flatMap { inScope =>
            editUpdate[E].edit(inScope, spec, edit, (rep: dag.BucketSpec) => for { state <- monadState.gets(identity) } yield rep,
              (_: dag.BucketSpec) match {
                case dag.UnionBucketSpec(left, right) =>
                  for {
                    newLeft <- memoizedSpec(left)
                    newRight <- memoizedSpec(right)
                  } yield dag.UnionBucketSpec(newLeft, newRight)
                
                case dag.IntersectBucketSpec(left, right) =>
                  for {
                    newLeft <- memoizedSpec(left)
                    newRight <- memoizedSpec(right)
                  } yield dag.IntersectBucketSpec(newLeft, newRight)
                
                case dag.Group(id, target, child) =>
                  for {
                    newTarget <- memoized(target)
                    newChild <- memoizedSpec(child)
                  } yield dag.Group(id, newTarget, newChild)
                
                case dag.UnfixedSolution(id, target) =>
                  for { newTarget <- memoized(target) } yield dag.UnfixedSolution(id, newTarget)
                
                case dag.Extra(target) =>
                  for { newTarget <- memoized(target) } yield dag.Extra(newTarget)
              }
            )
          }
          
        if(spec != scope)
          rewritten
        else
          for {
            node <- rewritten
            _ <- monadState.modify(_.copy(rewrittenScope = scopeUpdate[S].update(node)))
          } yield node
      }
      
      val resultM =
        for {
          preMemo <- editUpdate[E].bimap(edit._2)(memoized _, memoizedSpec _)
          result <- memoized(this)
        } yield result
      
      val (state, graph) = resultM(init)
      (graph, state.rewrittenScope.getOrElse(scope))
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

        case dag.SplitGroup(_, identities, _) => acc

        case node @ dag.Const(_) => acc

        case dag.Undefined() => acc

        case dag.New(parent) => foldDown0(parent, acc |+| f(parent))

        case dag.Morph1(_, parent) => foldDown0(parent, acc |+| f(parent))

        case dag.Morph2(_, left, right) => 
          val acc2 = foldDown0(left, acc |+| f(left))
          foldDown0(right, acc2 |+| f(right))

        case dag.Distinct(parent) => foldDown0(parent, acc |+| f(parent))

        case dag.LoadLocal(parent, _) => foldDown0(parent, acc |+| f(parent))

        case dag.Operate(_, parent) => foldDown0(parent, acc |+| f(parent))

        case node @ dag.Reduce(_, parent) => foldDown0(parent, acc |+| f(parent))

        case node @ dag.MegaReduce(_, parent) => foldDown0(parent, acc |+| f(parent))

        case dag.Split(specs, child, _) =>
          val specsAcc = foldThroughSpec(specs, acc)
          if (enterSplitChild)
            foldDown0(child, specsAcc |+| f(child))
          else
            specsAcc

        case dag.Assert(pred, child) =>
          val acc2 = foldDown0(pred, acc |+| f(pred))
          foldDown0(child, acc2 |+| f(child))

        case dag.Cond(pred, left, _, right, _) =>
          val acc2 = foldDown0(pred, acc |+| f(pred))
          val acc3 = foldDown0(left, acc2 |+| f(left))
          foldDown0(right, acc3 |+| f(right))

        case dag.Observe(data, samples) =>
          val acc2 = foldDown0(data, acc |+| f(data))
          foldDown0(samples, acc2 |+| f(samples))

        case dag.IUI(_, left, right) =>
          val acc2 = foldDown0(left, acc |+| f(left))
          foldDown0(right, acc2 |+| f(right))

        case dag.Diff(left, right) =>
          val acc2 = foldDown0(left, acc |+| f(left))
          foldDown0(right, acc2 |+| f(right))

        case dag.Join(_, _, left, right) =>
          val acc2 = foldDown0(left, acc |+| f(left))
          foldDown0(right, acc2 |+| f(right))

        case dag.Filter(_, target, boolean) =>
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
      def unapply(graph: Const): Option[String] = graph match {
        case Const(CString(str)) => Some(str)
        case _ => None
      }
    }

    object ConstDecimal {
      def unapply(graph: Const): Option[BigDecimal] = graph match {
        case Const(CNum(d)) => Some(d)
        case Const(CLong(d)) => Some(d)
        case Const(CDouble(d)) => Some(d)
        case _ => None
      }
    }
    
    //tic variable node
    case class SplitParam(id: Int, parentId: Identifier)(val loc: Line) extends DepGraph {
      val identities = Identities.Specs.empty
      
      val sorting = IdentitySort
      
      val isSingleton = true
      
      val containsSplitArg = true
    }
    
    //grouping node (e.g. foo where foo.a = 'b)
    case class SplitGroup(id: Int, identities: Identities, parentId: Identifier)(val loc: Line) extends DepGraph {
      val sorting = IdentitySort
      
      val isSingleton = false
      
      val containsSplitArg = true
    }
    
    case class Const(value: RValue)(val loc: Line) extends DepGraph with Root {
      lazy val identities = Identities.Specs.empty
      
      val sorting = IdentitySort
      
      val isSingleton = true
      
      val containsSplitArg = false
    }

    // TODO
    class Undefined(val loc: Line) extends DepGraph with Root {
      lazy val identities = Identities.Undefined

      val sorting = IdentitySort

      val isSingleton = false

      val containsSplitArg = false
      
      override def equals(that: Any) = that match {
        case that: Undefined => true
        case _ => false
      }
      
      override def hashCode = 42
    }
    
    object Undefined {
      def apply(loc: Line): Undefined = new Undefined(loc)
      def unapply(undef: Undefined): Boolean = true
    }
    
    case class New(parent: DepGraph)(val loc: Line) extends DepGraph {
      lazy val identities = Identities.Specs(Vector(SynthIds(IdGen.nextInt())))
      
      val sorting = IdentitySort
      
      lazy val isSingleton = parent.isSingleton
      
      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class Morph1(mor: Morphism1, parent: DepGraph)(val loc: Line) extends DepGraph with StagingPoint {
      lazy val identities = mor.idPolicy match {
        case _: IdentityPolicy.Retain => parent.identities
        
        case IdentityPolicy.Synthesize => Identities.Specs(Vector(SynthIds(IdGen.nextInt())))
        
        case IdentityPolicy.Strip => Identities.Specs.empty
      }
      
      val sorting = IdentitySort
      
      lazy val isSingleton = false
      
      lazy val containsSplitArg = parent.containsSplitArg
    }

    case class Morph2(mor: Morphism2, left: DepGraph, right: DepGraph)(val loc: Line) extends DepGraph with StagingPoint {
      lazy val identities = mor.idPolicy match {
        case IdentityPolicy.Retain.Left =>
          left.identities
        
        case IdentityPolicy.Retain.Right =>
          right.identities
        
        case IdentityPolicy.Retain.Merge => {
          // backwards compatibility with idAlignment
          if (mor.idAlignment == IdentityAlignment.MatchAlignment)
            (left.identities ++ right.identities).distinct
          else if (mor.idAlignment == IdentityAlignment.RightAlignment)
            right.identities
          else if (mor.idAlignment == IdentityAlignment.LeftAlignment)
            left.identities
          else
            left.identities ++ right.identities
        }
        
        case IdentityPolicy.Synthesize => Identities.Specs(Vector(SynthIds(IdGen.nextInt())))
        
        case IdentityPolicy.Strip => Identities.Specs.empty
      }
      
      val sorting = IdentitySort
      
      lazy val isSingleton = false
      
      lazy val containsSplitArg = left.containsSplitArg || right.containsSplitArg
    }

    case class Distinct(parent: DepGraph)(val loc: Line) extends DepGraph with StagingPoint {
      lazy val identities = Identities.Specs(Vector(SynthIds(IdGen.nextInt())))
      
      val sorting = IdentitySort
      
      lazy val isSingleton = parent.isSingleton
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class LoadLocal(parent: DepGraph, jtpe: JType = JType.JUniverseT)(val loc: Line) extends DepGraph with StagingPoint {
      lazy val identities = parent match {
        case Const(CString(path)) => Identities.Specs(Vector(LoadIds(path)))
        case Morph1(expandGlob, Const(CString(path))) => Identities.Specs(Vector(LoadIds(path)))
        case _ => Identities.Specs(Vector(SynthIds(IdGen.nextInt())))
      }
      
      val sorting = IdentitySort
      
      val isSingleton = false
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class Operate(op: UnaryOperation, parent: DepGraph)(val loc: Line) extends DepGraph {
      lazy val identities = parent.identities
      
      lazy val sorting = parent.sorting
      
      lazy val isSingleton = parent.isSingleton
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class Reduce(red: Reduction, parent: DepGraph)(val loc: Line) extends DepGraph with StagingPoint {
      lazy val identities = Identities.Specs.empty
      
      val sorting = IdentitySort
      
      val isSingleton = true
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class MegaReduce(reds: List[(TS1, List[Reduction])], parent: DepGraph) extends DepGraph with StagingPoint {
      val loc = parent.loc
      
      lazy val identities = Identities.Specs.empty
      
      val sorting = IdentitySort
      
      val isSingleton = false
      
      lazy val containsSplitArg = parent.containsSplitArg
    }
    
    case class Split(spec: BucketSpec, child: DepGraph, id: Identifier)(val loc: Line) extends DepGraph with StagingPoint {
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
    
    case class Assert(pred: DepGraph, child: DepGraph)(val loc: Line) extends DepGraph {
      lazy val identities = child.identities
      
      val sorting = child.sorting
      
      lazy val isSingleton = child.isSingleton
      
      lazy val containsSplitArg = pred.containsSplitArg || child.containsSplitArg
    }
    
    // note: this is not a StagingPoint, though it *could* be; this is an optimization for the common case (transpecability)
    case class Cond(pred: DepGraph, left: DepGraph, leftJoin: JoinSort, right: DepGraph, rightJoin: JoinSort)(val loc: Line) extends DepGraph {
      val peer = IUI(true, Filter(leftJoin, left, pred)(loc), Filter(rightJoin, right, Operate(Comp, pred)(loc))(loc))(loc)
      
      lazy val identities = peer.identities
      
      val sorting = peer.sorting
      
      lazy val isSingleton = peer.isSingleton
      
      lazy val containsSplitArg = peer.containsSplitArg
    }
    
    case class Observe(data: DepGraph, samples: DepGraph)(val loc: Line) extends DepGraph {
      lazy val identities = data.identities
      
      val sorting = data.sorting
      
      lazy val isSingleton = data.isSingleton
      
      lazy val containsSplitArg = data.containsSplitArg || samples.containsSplitArg
    }
    
    case class IUI(union: Boolean, left: DepGraph, right: DepGraph)(val loc: Line) extends DepGraph with StagingPoint {
      lazy val identities = (left.identities, right.identities) match {
        case (Identities.Specs(a), Identities.Specs(b)) => Identities.Specs((a, b).zipped map CoproductIds)
        case _ => Identities.Undefined
      }

      val sorting = IdentitySort    // TODO not correct!
      
      lazy val isSingleton = left.isSingleton && right.isSingleton
      
      lazy val containsSplitArg = left.containsSplitArg || right.containsSplitArg
    }
    
    case class Diff(left: DepGraph, right: DepGraph)(val loc: Line) extends DepGraph with StagingPoint {
      lazy val identities = left.identities
      
      val sorting = IdentitySort    // TODO not correct!
      
      lazy val isSingleton = left.isSingleton && right.isSingleton
      
      lazy val containsSplitArg = left.containsSplitArg || right.containsSplitArg
    }
    
    // TODO propagate AOT value computation
    case class Join(op: BinaryOperation, joinSort: JoinSort, left: DepGraph, right: DepGraph)(val loc: Line) extends DepGraph {
      lazy val identities = joinSort match {
        case CrossRightSort => right.identities ++ left.identities
        case CrossLeftSort => left.identities ++ right.identities
        
        case _ => (left.identities ++ right.identities).distinct
      }
      
      lazy val sorting = joinSort match {
        case tbl: TableSort => tbl
        case CrossLeftSort => left.sorting // This isn't correct.
        case CrossRightSort => right.sorting
      }
      
      lazy val isSingleton = left.isSingleton && right.isSingleton
      
      lazy val containsSplitArg = left.containsSplitArg || right.containsSplitArg
    }
    
    case class Filter(joinSort: JoinSort, target: DepGraph, boolean: DepGraph)(val loc: Line) extends DepGraph {
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
    
    
    sealed trait IdentitySpec {
      def canonicalize: IdentitySpec = this
    }

    case class LoadIds(path: String) extends IdentitySpec
    case class SynthIds(id: Int) extends IdentitySpec
    
    case class CoproductIds(left: IdentitySpec, right: IdentitySpec) extends IdentitySpec {
      override def canonicalize = {
        val left2 = left.canonicalize
        val right2 = right.canonicalize
        val this2 = CoproductIds(left2, right2)
        
        val pos = this2.possibilities
        
        pos reduceOption CoproductIds orElse pos.headOption getOrElse this2
      }
      
      private def possibilities: Set[IdentitySpec] = {
        val leftPos = left match {
          case left: CoproductIds => left.possibilities
          case _ => Set(left)
        }
        
        val rightPos = right match {
          case right: CoproductIds => right.possibilities
          case _ => Set(right)
        }
        
        leftPos ++ rightPos
      }
    }

    
    sealed trait JoinSort
    sealed trait TableSort extends JoinSort
    
    case object IdentitySort extends TableSort
    case class ValueSort(id: Int) extends TableSort
    // case object NullSort extends TableSort -- Not USED!
    
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
