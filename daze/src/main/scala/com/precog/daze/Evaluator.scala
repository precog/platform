package com.precog
package daze

import memoization._
import com.precog.yggdrasil._
import com.precog.yggdrasil.serialization._
import com.precog.util._
import com.precog.common.{Path, VectorCase}

import org.joda.time._
import org.joda.time.format._
import org.joda.time.DateTimeZone

import java.lang.Math._

import akka.dispatch.{Await, Future}
import akka.util.duration._
import blueeyes.json.{JPathField, JPathIndex}

import scalaz.{Identity => _, NonEmptyList => NEL, _}
import scalaz.effect._
import scalaz.syntax.traverse._
import scalaz.std.list._
import scalaz.std.partialFunction._

trait IdSource {
  def nextId(): Long
}

trait EvaluatorConfig {
  implicit def valueSerialization: SortSerialization[SValue]
  implicit def eventSerialization: SortSerialization[(Identities, SValue)]
  implicit def groupSerialization: SortSerialization[(SValue, Identities, SValue)]
  implicit def memoSerialization: IncrementalSerialization[(Identities, SValue)]
  def maxEvalDuration: akka.util.Duration
  def idSource: IdSource
}

trait Evaluator extends DAG
    with CrossOrdering
    with Memoizer
    with MatchAlgebra
    with OperationsAPI
    with MemoizationEnvironment
    with ImplLibrary
    with InfixLib
    with StatsLib
    with YggConfigComponent { self =>
  
  import Function._
  
  import instructions._
  import dag._

  type Dataset[E]
  type Memoable[E]
  type Grouping[K, A]
  type YggConfig <: EvaluatorConfig 

  sealed trait Context {
    def memoizationContext: MemoContext
    def expiration: Long
    def nextId(): Long
    def release: Release
  }

  implicit def asyncContext: akka.dispatch.ExecutionContext

  implicit def extend[E](d: Dataset[E]): DatasetExtensions[Dataset, Memoable, Grouping, E] = ops.extend(d)

  def withContext[A](f: Context => A): A = {
    withMemoizationContext { memoContext => 
      val ctx = new Context { 
        val memoizationContext = memoContext
        val expiration = System.currentTimeMillis + yggConfig.maxEvalDuration.toMillis 
        def nextId() = yggConfig.idSource.nextId()
        val release = new Release(IO())
      }

      f(ctx)
    }
  }

  import yggConfig._

  implicit val valueOrder: (SValue, SValue) => Ordering = Order[SValue].order _
  
  def eval(userUID: String, graph: DepGraph, ctx: Context): Dataset[SValue] = {
    def maybeRealize(result: Either[DatasetMask[Dataset], Match], graph: DepGraph, ctx: Context): Match =
      (result.left map { m => Match(mal.Actual, m.realize(ctx.expiration, ctx.release), graph) }).fold(identity, identity)
    
    def realizeMatch(spec: MatchSpec, set: Dataset[SValue]): Dataset[SValue] = spec match {
      case mal.Actual => set
      case _ => set collect resolveMatch(spec)
    }

    def computeGrouping(assume: Map[DepGraph, Match], splits: Map[dag.Split, Vector[Dataset[SValue]]], graph: DepGraph, ctx: Context)(spec: BucketSpec): Grouping[SValue, NEL[Dataset[SValue]]] = spec match {
      case ZipBucketSpec(left, right) => {
        val leftGroup = computeGrouping(assume, splits, graph, ctx)(left)
        val rightGroup = computeGrouping(assume, splits, graph, ctx)(right)
        ops.zipGroups(leftGroup, rightGroup)
      }
      
      case _: MergeBucketSpec | _: SingleBucketSpec =>
        ops.mapGrouping[SValue, Dataset[SValue], NEL[Dataset[SValue]]](computeMergeGrouping(assume, splits, graph, ctx)(spec)) { a => NEL(a) }
    }
    
    def computeMergeGrouping(assume: Map[DepGraph, Match], splits: Map[dag.Split, Vector[Dataset[SValue]]], graph: DepGraph, ctx: Context)(spec: BucketSpec): Grouping[SValue, Dataset[SValue]] = spec match {
      case ZipBucketSpec(_, _) => sys.error("Cannot merge_buckets following a zip_buckets")
      
      case MergeBucketSpec(left, right, and) => {
        val leftGroup = computeMergeGrouping(assume, splits, graph, ctx)(left)
        val rightGroup = computeMergeGrouping(assume, splits, graph, ctx)(right)
        ops.mergeGroups(leftGroup, rightGroup, !and, ctx.memoizationContext)
      }
      
      case SingleBucketSpec(target, solution) => {
        val common: DepGraph = findCommonality(Set())(target, solution) getOrElse sys.error("Case ruled out by Quirrel type checker")
        val Match(sourceSpec, sourceSet, _) = maybeRealize(loop(common, assume, splits, ctx), common, ctx)
        val source = realizeMatch(sourceSpec, sourceSet)
        
        val grouping = source.group[SValue](IdGen.nextInt(), ctx.memoizationContext) { sv: SValue =>
          val assume2 = assume + (common -> Match(mal.Actual, ops.point(sv), common))
          val Match(spec, set, _) = maybeRealize(loop(solution, assume2, splits, ctx), graph, ctx)
          realizeMatch(spec, set)
        }
        
        ops.mapGrouping(grouping) { bucket =>
          val assume2 = assume + (common -> Match(mal.Actual, bucket, common))
          val Match(spec, set, _) = maybeRealize(loop(target, assume2, splits, ctx), graph, ctx)
          realizeMatch(spec, set)
        }
      }
    }
    
    def findCommonality(seen: Set[DepGraph])(graphs: DepGraph*): Option[DepGraph] = {
      val (seen2, next, back) = graphs.foldLeft((seen, Vector[DepGraph](), None: Option[DepGraph])) {
        case (pair @ (_, _, Some(_)), _) => pair
        
        case ((seen, _, _), graph) if seen(graph) => (seen, Vector(), Some(graph))
        
        case ((oldSeen, next, None), graph) => {
          val seen = oldSeen + graph
          
          graph match {
            case SplitParam(_, _) | SplitGroup(_, _, _) | Root(_, _) | dag.Split(_, _, _) =>
              (seen, next, None)
              
            case dag.New(_, parent) =>
              (seen, next :+ parent, None)
            
            case dag.SetReduce(_, _, parent) =>
              (seen, next :+ parent, None)
            
            case dag.LoadLocal(_, _, parent, _) =>
              (seen, next :+ parent, None)
            
            case Operate(_, _, parent) =>
              (seen, next :+ parent, None)
            
            case dag.Reduce(_, _, parent) =>
              (seen, next :+ parent, None)
            
            case Join(_, _, left, right) =>
              (seen, next :+ left :+ right, None)
            
            case Filter(_, _, _, target, boolean) =>
              (seen, next :+ target :+ boolean, None)
            
            case Sort(parent, _) =>
              (seen, next :+ parent, None)
            
            case Memoize(parent, _) =>
              (seen, next :+ parent, None)
          }
        }
      }
      
      if (back.isDefined || next.isEmpty)
        back
      else
        findCommonality(seen2)(next: _*)
    }
  
    def loop(graph: DepGraph, assume: Map[DepGraph, Match], splits: Map[dag.Split, Vector[Dataset[SValue]]], ctx: Context): Either[DatasetMask[Dataset], Match] = graph match {
      case g if assume contains g => Right(assume(g))
      
      case s @ SplitParam(_, index) =>
        Right(Match(mal.Actual, splits(s.parent)(index), s))
      
      case s @ SplitGroup(_, index, _) =>
        Right(Match(mal.Actual, splits(s.parent)(index), s))
      
      case Root(_, instr) =>
        Right(Match(mal.Actual, ops.point(graph.value.get), graph))    // TODO don't be stupid
      
      case dag.New(_, parent) =>{
        val Match(spec, set, _) = maybeRealize(loop(parent, assume, splits, ctx), parent, ctx)
        Right(Match(mal.Actual, realizeMatch(spec, set) identify Some(() => ctx.nextId), graph))
      }
      
      case dag.LoadLocal(_, _, parent, _) => {    // TODO we can do better here
        parent.value match {
          case Some(SString(str)) => Left(query.mask(userUID, Path(str)))
          case Some(_) => Right(Match(mal.Actual, ops.empty[SValue](1), graph))
          
          case None => {
            val Match(spec, set, _) = maybeRealize(loop(parent, assume, splits, ctx), parent, ctx)
            val loaded = realizeMatch(spec, set) collect { 
              case SString(str) => query.fullProjection(userUID, Path(str), ctx.expiration, ctx.release)
            } 

            Right(Match(mal.Actual, ops.flattenAndIdentify(loaded, () => ctx.nextId()), graph))
          }
        }
      }

      case dag.SetReduce(_, Distinct, parent) => {
        val Match(spec, set, _) = maybeRealize(loop(parent, assume, splits, ctx), parent, ctx)
        val result = realizeMatch(spec, set).uniq(() => ctx.nextId(), IdGen.nextInt(), ctx.memoizationContext)
        Right(Match(mal.Actual, result, graph))
      }
      
      case o @ Operate(_, op, parent) => {
        // TODO unary typing
        val Match(spec, set, _) = maybeRealize(loop(parent, assume, splits, ctx), parent, ctx)
        lazy val enum = realizeMatch(spec, set)

        op match {
          case BuiltInFunction1Op(Rank) => { // (3,7,7,7,9,12,12,15) -> (1,2,2,2,5,6,6,8)
            var countTotal = 0
            var countEach = 1
            var previous: Option[SValue] = Option.empty[SValue]

            val enum2 = enum.sortByValue(o.memoId, ctx.memoizationContext)
            val enum3: Dataset[SValue] = enum2 collect {
              case s @ SDecimal(v) => {
                if (Some(s) == previous) {
                  previous = Some(s)
                  countEach += 1

                  SDecimal(countTotal)
                } else {
                  previous = Some(s)
                  countTotal += countEach 
                  countEach = 1
                
                  SDecimal(countTotal)
                }
              }
            }
            val enum4 = enum3.sortByIdentity(IdGen.nextInt, ctx.memoizationContext)

            Right(Match(mal.Op1(spec, op), enum4, graph))  
          }

          case BuiltInFunction1Op(DenseRank) => {  // (2,7,7,7,9,12,12,15) -> (1,2,2,2,3,4,4,5)
            var count = 0
            var previous: Option[SValue] = Option.empty[SValue]

            val enum2 = enum.sortByValue(o.memoId, ctx.memoizationContext)
            val enum3: Dataset[SValue] = enum2 collect {
              case s @ SDecimal(v) => {
                if (Some(s) == previous) {
                  previous = Some(s)

                  SDecimal(count)
                } else {
                  previous = Some(s)
                  count += 1

                  SDecimal(count)
                }
              }
            }
            val enum4 = enum3.sortByIdentity(IdGen.nextInt, ctx.memoizationContext)

            Right(Match(mal.Op1(spec, op), enum4, graph))  
          }

          case _ => Right(Match(mal.Op1(spec, op), set, graph))
        }
      }
      
      case r @ dag.Reduce(_, red, parent) => {
        val Match(spec, set, _) = maybeRealize(loop(parent, assume, splits, ctx), parent, ctx)

        val reduction = red match {
          case BuiltInReduction(red) => {
            val enum = realizeMatch(spec, set)
            red.reduced(enum, r, ctx)
          }
        }

        reduction.map { r => Right(Match(mal.Actual, ops.point[SValue](r), graph)) }.getOrElse(Right(Match(mal.Actual, ops.empty[SValue](0), graph)))
      }
      
      case s @ dag.Split(line, specs, child) => {
        def flattenAllGroups(groupings: Vector[Grouping[SValue, NEL[Dataset[SValue]]]], params: Vector[Dataset[SValue]], memoIds: Vector[Int]): Dataset[SValue] = {
          val current = groupings.head
          val rest = groupings.tail
          
          ops.flattenGroup(current, () => ctx.nextId(), memoIds.head, ctx.memoizationContext) { (key, groups) =>
            val params2 = ops.point(key) +: (Vector(groups.toList: _*) ++ params)
            
            if (rest.isEmpty) {
              val Match(spec, set, _) = maybeRealize(loop(child, assume, splits + (s -> params2), ctx), child, ctx)
              val back = realizeMatch(spec, set)
              
              // TODO this is probably not safe, since result Iterable might depend on something memoized
              for (id <- child.findMemos(s)) {
                ctx.memoizationContext.cache.expire(id)
              }
              // NOTE: Nope, it's not safe; it breaks the evaluator.
              
              back
            } else {
              flattenAllGroups(rest, params2, memoIds.tail)
            }
          }
        }
        
        val groupings = specs map computeGrouping(assume, splits, graph, ctx)
        Right(Match(mal.Actual, flattenAllGroups(groupings, Vector(), s.memoIds), s))
      }
      
      // VUnion and VIntersect removed, TODO: remove from bytecode
      
      case Join(_, instr @ (IUnion | IIntersect), left, right) => {
        val Match(leftSpec, leftSet, _) = maybeRealize(loop(left, assume, splits, ctx), left, ctx)
        val Match(rightSpec, rightSet, _) = maybeRealize(loop(right, assume, splits, ctx), right, ctx)
        
        val leftEnum = realizeMatch(leftSpec, leftSet)
        val rightEnum = realizeMatch(rightSpec, rightSet)
        
        val back = instr match { 
          case IUnion if left.provenance.length == right.provenance.length =>
            leftEnum.union(rightEnum, ctx.memoizationContext)
          
          // apparently Dataset tracks number of identities...
          case IUnion /* if left.provenance.length != right.provenance.length */ =>
            ops.empty[SValue](0)

          case IIntersect if left.provenance.length == right.provenance.length =>
            leftEnum.intersect(rightEnum, ctx.memoizationContext)
          
          case IIntersect /* if left.provenance.length != right.provenance.length */ =>
            ops.empty[SValue](0)
        }
        
        Right(Match(mal.Actual, back, graph))
      }
      
      case Join(_, Map2Cross(DerefObject) | Map2CrossLeft(DerefObject) | Map2CrossRight(DerefObject), left, right) if right.value.isDefined => {
        right.value match {
          case Some(value @ SString(str)) => {
            val parent = loop(left, assume, splits, ctx)
            val part1 = parent.left map { _ derefObject str }
            
            part1.right map {
              case Match(spec, set, graph) => Match(mal.Op2Single(spec, value, DerefObject, true), set, graph)
            }
          }
          
          case _ => Right(Match(mal.Actual, ops.empty[SValue](left.provenance.length), graph))
        }
      }
      
      case Join(_, Map2Cross(DerefArray) | Map2CrossLeft(DerefArray) | Map2CrossRight(DerefArray), left, right) if right.value.isDefined => {
        right.value match {
          case Some(value @ SDecimal(num)) if num.isValidInt => {
            val parent = loop(left, assume, splits, ctx)
            val part1 = parent.left map { _ derefArray num.toInt }
            
            part1.right map {
              case Match(spec, set, graph) => Match(mal.Op2Single(spec, value, DerefArray, true), set, graph)
            }
          }
          
          case _ => Right(Match(mal.Actual, ops.empty[SValue](left.provenance.length), graph))
        }
      }

      case Join(_, Map2Cross(op @ BuiltInFunction2Op(f)), left, right) if f.requiresReduction => {  
        val length = sharedPrefixLength(left, right)
        val bif = binaryOp(op)

        val Match(leftSpec, leftSet, _) = maybeRealize(loop(left, assume, splits, ctx), left, ctx)
        val Match(rightSpec, rightSet, _) = maybeRealize(loop(right, assume, splits, ctx), left, ctx)

        val leftEnum = realizeMatch(leftSpec, leftSet)
        val rightEnum = realizeMatch(rightSpec, rightSet)

        val tupleEnum = leftEnum.join(rightEnum, length)(bif.operation)
        val reduction = f.reduced(tupleEnum)

        reduction.map { r => Right(Match(mal.Actual, ops.point[SValue](r), graph)) }.getOrElse(Right(Match(mal.Actual, ops.empty[SValue](0), graph)))
      }

      case Join(_, Map2CrossLeft(op @ BuiltInFunction2Op(f)), left, right) if f.requiresReduction => {
        val length = sharedPrefixLength(left, right)
        val bif = binaryOp(op)

        val Match(leftSpec, leftSet, _) = maybeRealize(loop(left, assume, splits, ctx), left, ctx)
        val Match(rightSpec, rightSet, _) = maybeRealize(loop(right, assume, splits, ctx), left, ctx)

        val leftEnum = realizeMatch(leftSpec, leftSet)
        val rightEnum = realizeMatch(rightSpec, rightSet)

        val tupleEnum = leftEnum.join(rightEnum, length)(bif.operation)
        val reduction = f.reduced(tupleEnum)

        reduction.map { r => Right(Match(mal.Actual, ops.point[SValue](r), graph)) }.getOrElse(Right(Match(mal.Actual, ops.empty[SValue](0), graph)))
      }

      case Join(_, Map2CrossRight(op @ BuiltInFunction2Op(f)), left, right) if f.requiresReduction => {
        val length = sharedPrefixLength(left, right)
        val bif = binaryOp(op)

        val Match(leftSpec, leftSet, _) = maybeRealize(loop(left, assume, splits, ctx), left, ctx)
        val Match(rightSpec, rightSet, _) = maybeRealize(loop(right, assume, splits, ctx), left, ctx)

        val leftEnum = realizeMatch(leftSpec, leftSet)
        val rightEnum = realizeMatch(rightSpec, rightSet)

        val tupleEnum = leftEnum.join(rightEnum, length)(bif.operation)
        val reduction = f.reduced(tupleEnum)

        reduction.map { r => Right(Match(mal.Actual, ops.point[SValue](r), graph)) }.getOrElse(Right(Match(mal.Actual, ops.empty[SValue](0), graph)))
      }

      case Join(_, Map2CrossLeft(op), left, right) if right.isSingleton => {
        lazy val Match(leftSpec, leftSet, leftGraph2) = maybeRealize(loop(left, assume, splits, ctx), left, ctx)
        lazy val Match(rightSpec, rightSet, rightGraph2) = maybeRealize(loop(right, assume, splits, ctx), left, ctx)

        val rightEnum = realizeMatch(rightSpec, rightSet)

        val back = rightEnum.lastOption match {
          case Some(value) =>
            Match(mal.Op2Single(leftSpec, value, op, true), leftSet, leftGraph2)
          
          case None =>
            Match(mal.Actual, ops.empty[SValue](0), graph)
        }
        
        Right(back)
      }
      
      case Join(_, Map2CrossRight(op), left, right) if left.isSingleton => {
        lazy val Match(leftSpec, leftSet, leftGraph2) = maybeRealize(loop(left, assume, splits, ctx), left, ctx)
        lazy val Match(rightSpec, rightSet, rightGraph2) = maybeRealize(loop(right, assume, splits, ctx), left, ctx)

        val leftEnum = realizeMatch(leftSpec, leftSet)

        val back = leftEnum.lastOption match {
          case Some(value) =>
            Match(mal.Op2Single(rightSpec, value, op, false), rightSet, rightGraph2)
          
          case None =>
            Match(mal.Actual, ops.empty[SValue](0), graph)
        }
        
        Right(back)
      }
      
      // begin: annoyance with Scala's lousy pattern matcher
      case Join(_, Map2Cross(op), left, right) if right.value.isDefined => {
        val Match(spec, set, graph2) = maybeRealize(loop(left, assume, splits, ctx), left, ctx)
        Right(Match(mal.Op2Single(spec, right.value.get, op, true), set, graph2))
      }
      
      case Join(_, Map2CrossRight(op), left, right) if right.value.isDefined => {
        val Match(spec, set, graph2) = maybeRealize(loop(left, assume, splits, ctx), left, ctx)
        Right(Match(mal.Op2Single(spec, right.value.get, op, true), set, graph2))
      }      

      case Join(_, Map2CrossLeft(op), left, right) if right.value.isDefined => {
        val Match(spec, set, graph2) = maybeRealize(loop(left, assume, splits, ctx), left, ctx)
        Right(Match(mal.Op2Single(spec, right.value.get, op, true), set, graph2))
      }
      
      case Join(_, Map2Cross(op), left, right) if left.value.isDefined => {
        val Match(spec, set, graph2) = maybeRealize(loop(right, assume, splits, ctx), right, ctx)
        Right(Match(mal.Op2Single(spec, left.value.get, op, false), set, graph2))
      }

      case Join(_, Map2CrossRight(op), left, right) if left.value.isDefined => {
        val Match(spec, set, graph2) = maybeRealize(loop(right, assume, splits, ctx), right, ctx)
        Right(Match(mal.Op2Single(spec, left.value.get, op, false), set, graph2))
      }

      case Join(_, Map2CrossLeft(op), left, right) if left.value.isDefined => {
        val Match(spec, set, graph2) = maybeRealize(loop(right, assume, splits, ctx), right, ctx)
        Right(Match(mal.Op2Single(spec, left.value.get, op, false), set, graph2))
      }
      // end: annoyance
      
      case Join(_, Map2Match(op), left, right) => {
        lazy val length = sharedPrefixLength(left, right)
        val bif = binaryOp(op)
        
        val leftRes = loop(left, assume, splits, ctx)
        val rightRes = loop(right, assume, splits, ctx)
        
        val (leftTpe, rightTpe) = bif.operandType
        
        val leftResTyped = leftRes.left map { mask =>
          leftTpe map mask.typed getOrElse mask
        }
        
        val rightResTyped = rightRes.left map { mask =>
          rightTpe map mask.typed getOrElse mask
        }
        
        val Match(leftSpec, leftSet, leftGraph) = maybeRealize(leftResTyped, left, ctx)
        val Match(rightSpec, rightSet, rightGraph) = maybeRealize(rightResTyped, right, ctx)
        
        lazy val leftEnum = realizeMatch(leftSpec, leftSet)
        lazy val rightEnum = realizeMatch(rightSpec, rightSet)

        op match {
          case BuiltInFunction2Op(f) if f.requiresReduction => {
            lazy val tupleEnum = leftEnum.join(rightEnum, length)(bif.operation)

            lazy val reduction = f.reduced(tupleEnum)

            reduction.map { r => Right(Match(mal.Actual, ops.point[SValue](r), graph)) }.getOrElse(Right(Match(mal.Actual, ops.empty[SValue](0), graph)))
          }
          case _ => { 
            if (leftGraph == rightGraph) 
              Right(Match(mal.Op2Multi(leftSpec, rightSpec, op), leftSet, leftGraph))  
            else 
              Right(Match(mal.Actual, leftEnum.join(rightEnum, length)(bif.operation), graph))
          }
        }
      }

      case j @ Join(_, instr, left, right) => {
        lazy val length = sharedPrefixLength(left, right)
        
        val op = instr match {
          case Map2Cross(op) => binaryOp(op)
          case Map2CrossLeft(op) => binaryOp(op)
          case Map2CrossRight(op) => binaryOp(op)
        }
        
        val leftRes = loop(left, assume, splits, ctx)
        val rightRes = loop(right, assume, splits, ctx)
        
        val (leftTpe, rightTpe) = op.operandType
        
        val leftResTyped = leftRes.left map { mask =>
          leftTpe map mask.typed getOrElse mask
        }
        
        val rightResTyped = rightRes.left map { mask =>
          rightTpe map mask.typed getOrElse mask
        }
        
        val Match(leftSpec, leftSet, _) = maybeRealize(leftResTyped, left, ctx)
        val Match(rightSpec, rightSet, _) = maybeRealize(rightResTyped, right, ctx)
        
        val leftEnum = realizeMatch(leftSpec, leftSet)
        val rightEnum = realizeMatch(rightSpec, rightSet)
        
        val resultEnum = instr match {
          case Map2Cross(_) | Map2CrossLeft(_) =>
            val enum = if (right.isSingleton) rightEnum else rightEnum.memoize(right.memoId, ctx.memoizationContext)
            leftEnum.crossLeft(enum)(op.operation)
          
          case Map2CrossRight(_) =>
            val enum = if (left.isSingleton) leftEnum else leftEnum.memoize(left.memoId, ctx.memoizationContext)
            enum.crossRight(rightEnum)(op.operation)
        }
        
        Right(Match(mal.Actual, resultEnum, graph))
      }
      
      case Filter(_, None, _, target, boolean) => {
        lazy val length = sharedPrefixLength(target, boolean)
        
        val targetRes = loop(target, assume, splits, ctx)
        val booleanRes = loop(boolean, assume, splits, ctx)
        
        val booleanResTyped = booleanRes.left map { _ typed SBoolean }
        
        val Match(targetSpec, targetSet, targetGraph) = maybeRealize(targetRes, target, ctx)
        val Match(booleanSpec, booleanSet, booleanGraph) = maybeRealize(booleanResTyped, boolean, ctx)
        
        lazy val targetEnum = realizeMatch(targetSpec, targetSet)
        lazy val booleanEnum = realizeMatch(booleanSpec, booleanSet)
        
        if (targetGraph == booleanGraph)
          Right(Match(mal.Filter(targetSpec, booleanSpec), targetSet, targetGraph))
        else
          Right(Match(mal.Actual, targetEnum.join(booleanEnum, length) { case (sv, STrue) => sv }, graph))
      }
      
      case f @ Filter(_, Some(cross), _, target, boolean) => {
        lazy val length = sharedPrefixLength(target, boolean)
        
        val targetRes = loop(target, assume, splits, ctx)
        val booleanRes = loop(boolean, assume, splits, ctx)
        
        val booleanResTyped = booleanRes.left map { _ typed SBoolean }
        
        val Match(targetSpec, targetSet, _) = maybeRealize(targetRes, target, ctx)
        val Match(booleanSpec, booleanSet, _) = maybeRealize(booleanResTyped, boolean, ctx)
        
        val targetEnum = realizeMatch(targetSpec, targetSet)
        val booleanEnum = realizeMatch(booleanSpec, booleanSet)
        
        val resultEnum = cross match {
          case CrossNeutral | CrossLeft =>
            targetEnum.crossLeft(booleanEnum.memoize(boolean.memoId, ctx.memoizationContext)) { case (sv, STrue) => sv }
          
          case CrossRight =>
            targetEnum.memoize(target.memoId, ctx.memoizationContext).crossRight(booleanEnum) { case (sv, STrue) => sv }
        }
        
        Right(Match(mal.Actual, resultEnum, graph))
      }
      
      case s @ Sort(parent, indexes) => {
        loop(parent, assume, splits, ctx).right map {
          case Match(spec, set, _) => {
            val enum = realizeMatch(spec, set)
            Match(mal.Actual, enum.sortByIndexedIds(indexes, s.memoId, ctx.memoizationContext), graph)
          }
        }
      }
      
      case m @ Memoize(parent, _) => {
        loop(parent, assume, splits, ctx).right map {
          case Match(mal.Actual, enum, graph) =>
            Match(mal.Actual, enum.memoize(m.memoId, ctx.memoizationContext), graph)
          
          case m => m
        }
      }
    }
    
    val Match(spec, set, _) = maybeRealize(loop(memoize(orderCrosses(graph)), Map(), Map(), ctx), graph, ctx)
    realizeMatch(spec, set)
  }
  
  override def resolveUnaryOperation(op: UnaryOperation) = op match {
    case Comp => {
      case SBoolean(b) => SBoolean(!b)
    }
    
    case Neg => {
      case SDecimal(d) => SDecimal(-d)
    }
    
    case WrapArray => {
      case sv => SArray(Vector(sv))
    }
    
    case BuiltInFunction1Op(f) => f.operation
  }
  
  override def resolveBinaryOperation(op: BinaryOperation) = op match {
    case DerefObject => {
      case (sv, SString(str)) if SValue.deref(JPathField(str)).isDefinedAt(sv) =>
        SValue.deref(JPathField(str))(sv)
    }
    
    case DerefArray => {
      case (sv, SDecimal(num)) if SValue.deref(JPathIndex(num.toInt)).isDefinedAt(sv) =>
        SValue.deref(JPathIndex(num.toInt))(sv)
    }
    
    case op => binaryOp(op).operation
  }

  private def binaryOp(op: BinaryOperation): BIF2 = {
    op match {
      case Add => Infix.Add
      case Sub => Infix.Sub
      case Mul => Infix.Mul
      case Div => Infix.Div
      
      case Lt   => Infix.Lt
      case LtEq => Infix.LtEq
      case Gt   => Infix.Gt
      case GtEq => Infix.GtEq
      
      case Eq    => Infix.Eq
      case NotEq => Infix.NotEq
      
      case And => Infix.And
      case Or  => Infix.Or
      
      case WrapObject => Infix.WrapObject
      
      case JoinObject => Infix.JoinObject
      case JoinArray  => Infix.JoinArray
      
      case ArraySwap  => Infix.ArraySwap
      
      case DerefObject => Infix.DerefObject
      case DerefArray  => Infix.DerefArray

      case BuiltInFunction2Op(f) => f
    }
  }

  private def sharedPrefixLength(left: DepGraph, right: DepGraph): Int =
    left.provenance zip right.provenance takeWhile { case (a, b) => a == b } length
}
