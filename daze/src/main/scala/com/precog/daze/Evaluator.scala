package com.precog
package daze

import org.joda.time._
import org.joda.time.format._
import org.joda.time.DateTimeZone

import java.lang.Math._

import akka.dispatch.{Await, Future}
import akka.util.duration._
import blueeyes.json.{JPathField, JPathIndex}

import scalaz.{Identity => _, _}
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
  def chunkSerialization: FileSerialization[SValue]
  def maxEvalDuration: akka.util.Duration
  def idSource: IdSource
}


trait Evaluator extends DAG with CrossOrdering with Memoizer with OperationsAPI with MemoEnvironment 
with ImplLibrary with Infixlib with YggConfigComponent { self =>
  import Function._
  
  import instructions._
  import dag._

  type Dataset[E]
  type YggConfig <: EvaluatorConfig 

  trait Context {
    def memoizationContext: MemoContext
    def expiration: Long
    def nextId(): Long
  }

  implicit def asyncContext: akka.dispatch.ExecutionContext

  implicit def extend[E <: AnyRef](d: Dataset[E]): DatasetExtensions[Dataset, E] = ops.extend(d)

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

  lazy implicit val chunkSerialization = yggConfig.chunkSerialization

  implicit val valueOrder: (SValue, SValue) => Ordering = Order[SValue].order _
  
  def eval(userUID: String, graph: DepGraph): Dataset[SValue] = {
    def maybeRealize(result: Either[DatasetMask[Dataset], Dataset[SValue]], ctx: Context): Dataset[SValue] =
      (result.left map { _.realize(ctx.expiration) }).fold(identity, identity)
  
    def loop(graph: DepGraph, roots: List[Dataset[SValue]], ctx: Context): Either[DatasetMask[Dataset], Dataset[SValue]] = graph match {
      case SplitRoot(_, depth) => Right(roots(depth))
      
      case Root(_, instr) =>
        Right(ops.point(graph.value.get))    // TODO don't be stupid
      
      case dag.New(_, parent) => loop(parent, roots, ctx)
      
      case dag.LoadLocal(_, _, parent, _) => {    // TODO we can do better here
        parent.value match {
          case Some(SString(str)) => Left(query.mask(userUID, Path(str)))
          case Some(_) => Right(ops.empty[SValue])
          
          case None => {
            val loaded = maybeRealize(loop(parent, roots, ctx), ctx) collect { 
              case SString(str) => query.fullProjection(userUID, Path(str), ctx.expiration)
            } 

            Right(ops.flattenSorted(loaded, parent.provenance.length, IdGen.nextInt()))
          }
        }
      }

      case dag.SetReduce(_, Distinct, parent) => {  
        Right(maybeRealize(loop(parent, roots, ctx), ctx).uniq.identify(Some(() => ctx.nextId())))
      }
      
      case Operate(_, Comp, parent) => {
        val parentRes = loop(parent, roots, ctx)
        val parentResTyped = parentRes.left map { _ typed SBoolean }
        val enum = maybeRealize(parentResTyped, ctx)
        
        Right(enum collect {
          case SBoolean(b) => SBoolean(!b)
        })
      }
      
      case Operate(_, Neg, parent) => {
        val parentRes = loop(parent, roots, ctx)
        val parentResTyped = parentRes.left map { _ typed SDecimal }
        val enum = maybeRealize(parentResTyped, ctx)
        
        Right(enum collect {
          case SDecimal(d) => SDecimal(-d)
        })
      }
      
      case Operate(_, WrapArray, parent) => {
        val enum = maybeRealize(loop(parent, roots, ctx), ctx)
        
        Right(enum map { sv => SArray(Vector(sv)) })
      }

      case Operate(_, BuiltInFunction1Op(f), parent) => {
        implicit val pfArrow = partialFunctionInstance
        val parentRes = loop(parent, roots, ctx)
        val parentResTyped = parentRes.left map { mask =>
          f.operandType map mask.typed getOrElse mask
        }

        Right(maybeRealize(parentResTyped, ctx) collect f.operation)
      }
      
      // TODO mode and median
      case dag.Reduce(_, red, parent) => {
        val enum = maybeRealize(loop(parent, roots, ctx), ctx)
        
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
      
      case dag.Split(_, parent, child) => {
        val splitEnum: Dataset[SValue] = maybeRealize(loop(parent, roots, ctx), ctx)
        
        lazy val volatileMemos = child.findMemos filter { _ isVariable 0 }
        lazy val volatileIds = volatileMemos map { _.memoId }
        
        val result: Dataset[SValue] = ops.extend[SValue](splitEnum).uniq flatMap { sv => 
          maybeRealize(loop(child, ops.point(sv) :: roots, ctx), ctx) perform {
            (volatileIds map ctx.memoizationContext.cache.expire).toList.sequence
          }
        }
        
        Right(result.uniq.identify(Some(ctx.nextId)))
      }
      
      case Join(_, instr @ (VUnion | VIntersect), left, right) => {
        val leftEnum  = maybeRealize(loop(left, roots, ctx), ctx)
        val rightEnum = maybeRealize(loop(right, roots, ctx), ctx)
        
        Right(
          instr match {
            case VUnion     => leftEnum.union(rightEnum, false)
            case VIntersect => leftEnum.intersect(rightEnum, false)
          }
        )
      }
      
      case Join(_, instr @ (IUnion | IIntersect), left, right) => {
        val leftEnum = maybeRealize(loop(left, roots, ctx), ctx)
        val rightEnum = maybeRealize(loop(right, roots, ctx), ctx)

        Right(
          instr match {
            case VUnion     => leftEnum.union(rightEnum, true)
            case VIntersect => leftEnum.intersect(rightEnum, true)
          }
        )
      }
      
      case Join(_, Map2Cross(DerefObject) | Map2CrossLeft(DerefObject) | Map2CrossRight(DerefObject), left, right) if right.value.isDefined => {
        right.value.get match {
          case SString(str) => 
            loop(left, roots, ctx).fold(
               mask => Left(mask derefObject str),
               enum => Right(enum collect SValue.deref(JPathField(str)))
            )
          
          case _ => Right(ops.empty[SValue])
        }
      }
      
      case Join(_, Map2Cross(DerefArray) | Map2CrossLeft(DerefArray) | Map2CrossRight(DerefArray), left, right) if right.value.isDefined => {
        right.value.get match {
          case SDecimal(num) if num.isValidInt => 
            loop(left, roots, ctx).fold(
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
        
        val leftRes = loop(left, roots, ctx)
        val rightRes = loop(right, roots, ctx)
        
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
        
        val targetRes = loop(target, roots, ctx)
        val booleanRes = loop(boolean, roots, ctx)
        
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
        loop(parent, roots, ctx).right map { _.sortByIndexedIds(indexes, s.memoId) }
      
      case m @ Memoize(parent, _) =>
        loop(parent, roots, ctx).right map { _.memoize(m.memoId) }
    }
    
    withContext { ctx =>
      maybeRealize(loop(memoize(orderCrosses(graph)), Nil, ctx), ctx)
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
