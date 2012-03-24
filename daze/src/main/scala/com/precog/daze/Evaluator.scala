package com.precog
package daze

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
//import scalaz.syntax.monad._
import scalaz.std.list._
import scalaz.std.partialFunction._

import com.precog.yggdrasil._
import com.precog.util._
import com.precog.common.{Path, VectorCase}

trait IdSource {
  def nextId(): Long
}

trait EvaluatorConfig {
  implicit def valueSerialization: SortSerialization[SValue]
  implicit def eventSerialization: SortSerialization[(Identities, SValue)]
  implicit def keyValueSerialization: SortSerialization[(SValue, Identities, SValue)]
  def maxEvalDuration: akka.util.Duration
  def idSource: IdSource
}


trait Evaluator extends DAG
    with CrossOrdering
    with Memoizer
    with OperationsAPI
    with MemoEnvironment
    with ImplLibrary
    with Infixlib
    with YggConfigComponent { self =>
  
  import Function._
  
  import instructions._
  import dag._

  type Dataset[E]
  type Grouping[K, A]
  type YggConfig <: EvaluatorConfig 

  trait Context {
    def memoizationContext: MemoContext
    def expiration: Long
    def nextId(): Long
  }

  implicit def asyncContext: akka.dispatch.ExecutionContext

  implicit def extend[E <: AnyRef](d: Dataset[E]): DatasetExtensions[Dataset, Grouping, E] = ops.extend(d)

  def withContext(f: Context => Dataset[SValue]): Dataset[SValue] = {
    withMemoizationContext { memoContext => 
      val ctx = new Context { 
        val memoizationContext = memoContext
        val expiration = System.currentTimeMillis + yggConfig.maxEvalDuration.toMillis 
        def nextId() = yggConfig.idSource.nextId()
      }

      f(ctx).perform(memoContext.cache.purge)
    }
  }

  import yggConfig._
  implicit val valueOrder: (SValue, SValue) => Ordering = Order[SValue].order _
  
  def eval(userUID: String, graph: DepGraph): Dataset[SValue] = {
    def maybeRealize(result: Either[DatasetMask[Dataset], Dataset[SValue]], ctx: Context): Dataset[SValue] =
      (result.left map { _.realize(ctx.expiration) }).fold(identity, identity)
  
    def computeGrouping(assume: Map[DepGraph, Dataset[SValue]], roots: Map[dag.Split, Vector[Dataset[SValue]]], ctx: Context)(spec: BucketSpec): Grouping[SValue, NEL[Dataset[SValue]]] = spec match {
      case ZipBucketSpec(left, right) => {
        val leftGroup = computeGrouping(assume, roots, ctx)(left)
        val rightGroup = computeGrouping(assume, roots, ctx)(right)
        ops.zipGroups(leftGroup, rightGroup)
      }
      
      case _: MergeBucketSpec | _: SingleBucketSpec =>
        ops.mapGrouping(computeMergeGrouping(assume, roots, ctx)(spec)) { a => NEL(a) }
    }
    
    def computeMergeGrouping(assume: Map[DepGraph, Dataset[SValue]], roots: Map[dag.Split, Vector[Dataset[SValue]]], ctx: Context)(spec: BucketSpec): Grouping[SValue, Dataset[SValue]] = spec match {
      case ZipBucketSpec(_, _) => sys.error("Cannot merge_buckets following a zip_buckets")
      
      case MergeBucketSpec(left, right, and) => {
        val leftGroup = computeMergeGrouping(assume, roots, ctx)(left)
        val rightGroup = computeMergeGrouping(assume, roots, ctx)(right)
        ops.mergeGroups(leftGroup, rightGroup, !and)
      }
      
      case SingleBucketSpec(target, solution) => {
        val common: DepGraph = findCommonality(Set())(target, solution) getOrElse sys.error("Case ruled out by Quirrel type checker")
        val source: Dataset[SValue] = maybeRealize(loop(common, assume, roots, ctx), ctx)
        
        source.group[SValue](IdGen.nextInt()) { sv: SValue =>
          maybeRealize(loop(solution, assume + (common -> ops.point(sv)), roots, ctx), ctx)
        }
      }
    }
    
    def findCommonality(seen: Set[DepGraph])(graphs: DepGraph*): Option[DepGraph] = {
      val (seen2, next, back) = graphs.foldLeft((seen, Set[DepGraph](), None: Option[DepGraph])) {
        case (pair @ (_, _, Some(_)), _) => pair
        
        case ((seen, _, _), graph) if seen(graph) => (seen, Set(), Some(graph))
        
        case ((oldSeen, next, None), graph) => {
          val seen = oldSeen + graph
          
          graph match {
            case SplitParam(_, _) | SplitGroup(_, _, _) | Root(_, _) | dag.Split(_, _, _) =>
              (seen, next, None)
              
            case dag.New(_, parent) =>
              (seen, next + parent, None)
            
            case dag.SetReduce(_, _, parent) =>
              (seen, next + parent, None)
            
            case dag.LoadLocal(_, _, parent, _) =>
              (seen, next + parent, None)
            
            case Operate(_, _, parent) =>
              (seen, next + parent, None)
            
            case dag.Reduce(_, _, parent) =>
              (seen, next + parent, None)
            
            case Join(_, _, left, right) =>
              (seen, next + left + right, None)
            
            case Filter(_, _, _, target, boolean) =>
              (seen, next + target + boolean, None)
            
            case Sort(parent, _) =>
              (seen, next + parent, None)
            
            case Memoize(parent, _) =>
              (seen, next + parent, None)
          }
        }
      }
      
      if (back.isDefined)
        back
      else
        findCommonality(seen2)(next.toSeq: _*)
    }
  
    def loop(graph: DepGraph, assume: Map[DepGraph, Dataset[SValue]], roots: Map[dag.Split, Vector[Dataset[SValue]]], ctx: Context): Either[DatasetMask[Dataset], Dataset[SValue]] = graph match {
      case g if assume contains g => Right(assume(g))
      
      case s @ SplitParam(_, index) => Right(roots(s.parent)(index))
      
      case s @ SplitGroup(_, index, _) => Right(roots(s.parent)(index))
      
      case Root(_, instr) =>
        Right(ops.point(graph.value.get))    // TODO don't be stupid
      
      case dag.New(_, parent) => loop(parent, assume, roots, ctx)
      
      case dag.LoadLocal(_, _, parent, _) => {    // TODO we can do better here
        parent.value match {
          case Some(SString(str)) => Left(query.mask(userUID, Path(str)))
          case Some(_) => Right(ops.empty[SValue])
          
          case None => {
            val loaded = maybeRealize(loop(parent, assume, roots, ctx), ctx) collect { 
              case SString(str) => query.fullProjection(userUID, Path(str), ctx.expiration)
            } 

            Right(ops.flattenAndIdentify(loaded, () => ctx.nextId()))
          }
        }
      }

      case dag.SetReduce(_, Distinct, parent) => {  
        Right(maybeRealize(loop(parent, assume, roots, ctx), ctx).uniq(() => ctx.nextId(), IdGen.nextInt(), ctx.memoizationContext))
      }
      
      case Operate(_, Comp, parent) => {
        val parentRes = loop(parent, assume, roots, ctx)
        val parentResTyped = parentRes.left map { _ typed SBoolean }
        val enum = maybeRealize(parentResTyped, ctx)
        
        Right(enum collect {
          case SBoolean(b) => SBoolean(!b)
        })
      }
      
      case Operate(_, Neg, parent) => {
        val parentRes = loop(parent, assume, roots, ctx)
        val parentResTyped = parentRes.left map { _ typed SDecimal }
        val enum = maybeRealize(parentResTyped, ctx)
        
        Right(enum collect {
          case SDecimal(d) => SDecimal(-d)
        })
      }
      
      case Operate(_, WrapArray, parent) => {
        val enum = maybeRealize(loop(parent, assume, roots, ctx), ctx)
        
        Right(enum map { sv => SArray(Vector(sv)) })
      }

      case Operate(_, BuiltInFunction1Op(f), parent) => {
        implicit val pfArrow = partialFunctionInstance
        val parentRes = loop(parent, assume, roots, ctx)
        val parentResTyped = parentRes.left map { mask =>
          f.operandType map mask.typed getOrElse mask
        }

        Right(maybeRealize(parentResTyped, ctx) collect f.operation)
      }
      
      // TODO mode and median
      case dag.Reduce(_, red, parent) => {
        val enum = maybeRealize(loop(parent, assume, roots, ctx), ctx)
        
        val reduced = red match {
          case Count => ops.point(SDecimal(BigDecimal(enum.count)))
          
          case Max => 
            val max = enum.reduce(Option.empty[BigDecimal]) {
              case (None, SDecimal(v)) => Some(v)
              case (Some(v1), SDecimal(v2)) if v1 >= v2 => Some(v1)
              case (Some(v1), SDecimal(v2)) if v1 < v2 => Some(v2)
              case (acc, _) => acc
            }

            max.map(v => ops.point(SDecimal(v))).getOrElse(ops.empty[SValue])
          
          case Min => 
            val min = enum.reduce(Option.empty[BigDecimal]) {
              case (None, SDecimal(v)) => Some(v)
              case (Some(v1), SDecimal(v2)) if v1 <= v2 => Some(v1)
              case (Some(v1), SDecimal(v2)) if v1 > v2 => Some(v2)
              case (acc, _) => acc
            }
          
            min.map(v => ops.point(SDecimal(v))).getOrElse(ops.empty[SValue])
          
          case Sum => 
            val sum = enum.reduce(Option.empty[BigDecimal]) {
              case (None, SDecimal(v)) => Some(v)
              case (Some(sum), SDecimal(v)) => Some(sum + v)
              case (acc, _) => acc
            }

            sum.map(v => ops.point(SDecimal(v))).getOrElse(ops.empty[SValue])

          case Mean => 
            val (count, total) = enum.reduce((BigDecimal(0), BigDecimal(0))) {
              case ((count, total), SDecimal(v)) => (count + 1, total + v)
              case (total, _) => total
            }
            
            if (count == BigDecimal(0)) ops.empty[SValue]
            else ops.point(SDecimal(total / count))
          
          case GeometricMean => 
            val (count, total) = enum.reduce((BigDecimal(0), BigDecimal(0))) {
              case ((count, acc), SDecimal(v)) => (count + 1, acc * v)
              case (acc, _) => acc
            }
            
            if (count == BigDecimal(0)) ops.empty[SValue]
            else ops.point(SDecimal(Math.pow(total.toDouble, 1 / count.toDouble)))
          
          case SumSq => 
            val sumsq = enum.reduce(Option.empty[BigDecimal]) {
              case (None, SDecimal(v)) => Some(v * v)
              case (Some(sumsq), SDecimal(v)) => Some(sumsq + (v * v))
              case (acc, _) => acc
            }

            sumsq.map(v => ops.point(SDecimal(v))).getOrElse(ops.empty[SValue])

          case Variance => 
            val (count, sum, sumsq) = enum.reduce((BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
              case ((count, sum, sumsq), SDecimal(v)) => (count + 1, sum + v, sumsq + (v * v))
              case (acc, _) => acc
            }

            if (count == BigDecimal(0)) ops.empty[SValue]
            else ops.point(SDecimal((sumsq - (sum * (sum / count))) / count))

          case StdDev => 
            val (count, sum, sumsq) = enum.reduce((BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
              case ((count, sum, sumsq), SDecimal(v)) => (count + 1, sum + v, sumsq + (v * v))
              case (acc, _) => acc
            }
            
            if (count == BigDecimal(0)) ops.empty[SValue]
            else ops.point(SDecimal(sqrt(count * sumsq - sum * sum) / count))
        }
        
        Right(reduced)
      }
      
      case s @ dag.Split(_, specs, child) => {
        def flattenAllGroups(groupings: Vector[Grouping[SValue, NEL[Dataset[SValue]]]], params: Vector[Dataset[SValue]]): Dataset[SValue] = {
          val current = groupings.head
          val rest = groupings.tail
          
          ops.flattenGroup(current, () => ctx.nextId()) { (key, groups) =>
            val params2 = (ops.point(key) +: Vector(groups.toList: _*)) ++ params
            
            if (rest.isEmpty)
              maybeRealize(loop(child, assume, roots + (s -> params2), ctx), ctx)
            else
              flattenAllGroups(rest, params2)
          }
        }
        
        val groupings = specs map computeGrouping(assume, roots, ctx)
        Right(flattenAllGroups(groupings, Vector()))
      }
      
      // VUnion and VIntersect removed, TODO: remove from bytecode
      
      case Join(_, instr @ (IUnion | IIntersect), left, right) => {
        val leftEnum = maybeRealize(loop(left, assume, roots, ctx), ctx)
        val rightEnum = maybeRealize(loop(right, assume, roots, ctx), ctx)
        
        val back = instr match {
          case IUnion if left.provenance.length == right.provenance.length =>
            leftEnum.union(rightEnum)
          
          // apparently Dataset tracks number of identities...
          case IUnion if left.provenance.length != right.provenance.length =>
            leftEnum.paddedMerge(rightEnum, () => ctx.nextId(), IdGen.nextInt())
          
          case IIntersect if left.provenance.length == right.provenance.length =>
            leftEnum.intersect(rightEnum)
          
          case IIntersect if left.provenance.length != right.provenance.length =>
            ops.empty[SValue]
        }
        
        Right(back)
      }
      
      case Join(_, Map2Cross(DerefObject) | Map2CrossLeft(DerefObject) | Map2CrossRight(DerefObject), left, right) if right.value.isDefined => {
        right.value.get match {
          case SString(str) => 
            loop(left, assume, roots, ctx).fold(
               mask => Left(mask derefObject str),
               enum => Right(enum collect SValue.deref(JPathField(str)))
            )
          
          case _ => Right(ops.empty[SValue])
        }
      }
      
      case Join(_, Map2Cross(DerefArray) | Map2CrossLeft(DerefArray) | Map2CrossRight(DerefArray), left, right) if right.value.isDefined => {
        right.value.get match {
          case SDecimal(num) if num.isValidInt => 
            loop(left, assume, roots, ctx).fold(
              mask => Left(mask derefArray num.toInt),
              enum => Right(enum collect SValue.deref(JPathIndex(num.toInt)))
            )
          
          case _ => Right(ops.empty[SValue])
        }
      }
      
      case Join(_, instr, left, right) => {
        lazy val length = sharedPrefixLength(left, right)
        
        val op = instr match {
          case Map2Match(op) => binaryOp(op)
          case Map2Cross(op) => binaryOp(op)
          case Map2CrossLeft(op) => binaryOp(op)
          case Map2CrossRight(op) => binaryOp(op)
        }
        
        val leftRes = loop(left, assume, roots, ctx)
        val rightRes = loop(right, assume, roots, ctx)
        
        val (leftTpe, rightTpe) = op.operandType
        
        val leftResTyped = leftRes.left map { mask =>
          leftTpe map mask.typed getOrElse mask
        }
        
        val rightResTyped = rightRes.left map { mask =>
          rightTpe map mask.typed getOrElse mask
        }
        
        val leftEnum = maybeRealize(leftResTyped, ctx)
        val rightEnum = maybeRealize(rightResTyped, ctx)
        
        Right(
          instr match {
            case Map2Match(_) => leftEnum.join(rightEnum, length) { op.operation }
            case Map2Cross(_) | Map2CrossLeft(_) => leftEnum.crossLeft(rightEnum) { op.operation }
            case Map2CrossRight(_) => leftEnum.crossRight(rightEnum) { op.operation }
          }
        )
      }
      
      case Filter(_, cross, _, target, boolean) => {
        lazy val length = sharedPrefixLength(target, boolean)
        
        val targetRes = loop(target, assume, roots, ctx)
        val booleanRes = loop(boolean, assume, roots, ctx)
        
        val booleanResTyped = booleanRes.left map { _ typed SBoolean }
        
        val targetEnum = maybeRealize(targetRes, ctx)
        val booleanEnum = maybeRealize(booleanResTyped, ctx)
        
        Right(
          cross match {
            case None => targetEnum.join(booleanEnum, length) { case (sv, STrue) => sv }
            case Some(CrossNeutral) => targetEnum.crossLeft(booleanEnum) { case (sv, STrue) => sv }
            case Some(CrossLeft)    => targetEnum.crossLeft(booleanEnum) { case (sv, STrue) => sv }
            case Some(CrossRight)   => targetEnum.crossRight(booleanEnum) { case (sv, STrue) => sv }
          }
        )
      }
      
      case s @ Sort(parent, indexes) => 
        loop(parent, assume, roots, ctx).right map { _.sortByIndexedIds(indexes, s.memoId) }
      
      case m @ Memoize(parent, _) =>
        loop(parent, assume, roots, ctx).right map { _.memoize(m.memoId) }
    }
    
    withContext { ctx =>
      maybeRealize(loop(memoize(orderCrosses(graph)), Map(), Map(), ctx), ctx)
    }
  }

  /**
   * Newton's approximation to some number of iterations (by default: 50).
   * Ported from a Java example found here: http://www.java2s.com/Code/Java/Language-Basics/DemonstrationofhighprecisionarithmeticwiththeBigDoubleclass.htm
   */
  private[this] def sqrt(d: BigDecimal, k: Int = 50): BigDecimal = {
    lazy val approx = {   // could do this with a self map, but it would be much slower
      def gen(x: BigDecimal): Stream[BigDecimal] = {
        val x2 = (d + x * x) / (x * 2)
        
        lazy val tail = if (x2 == x)
          Stream.empty
        else
          gen(x2)
        
        x2 #:: tail
      }
      
      gen(d / 3)
    }
    
    approx take k last
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
