package com.precog
package daze

import bytecode._

import yggdrasil._
import yggdrasil.table._

import com.precog.util._

import blueeyes.json._

import scalaz._
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.std.tuple._
import scalaz.syntax.foldable._
import scalaz.syntax.std.option._
import scalaz.syntax.std.boolean._

import scala.annotation.tailrec
import scala.collection.mutable

object RangeUtil {
  /**
   * Loops through a Range much more efficiently than Range#foreach, running
   * the provided callback 'f' on each position. Assumes that step is 1.
   */
  def loop(r: Range)(f: Int => Unit) {
    var i = r.start
    val limit = r.end
    while (i < limit) {
      f(i)
      i += 1
    }
  }

  /**
   * Like loop but also includes a built-in check for whether the given Column
   * is defined for this particular row.
   */
  def loopDefined(r: Range, col: Column)(f: Int => Unit): Boolean = {
    @tailrec def unseen(i: Int, limit: Int): Boolean = if (i < limit) {
      if (col.isDefinedAt(i)) { f(i); seen(i + 1, limit) }
      else unseen(i + 1, limit)
    } else {
      false
    }

    @tailrec def seen(i: Int, limit: Int): Boolean = if (i < limit) {
      if (col.isDefinedAt(i)) f(i)
      seen(i + 1, limit)
    } else {
      true
    }

    unseen(r.start, r.end)
  }
}

class LongAdder {
  var t = 0L
  val ts = mutable.ArrayBuffer.empty[BigDecimal]

  final def maxLongSqrt = 3037000499L

  def add(x: BigDecimal): Unit = ts.append(x)

  def addSquare(x: Long) = if (x < maxLongSqrt)
    add(x * x)
  else
    add(BigDecimal(x) pow 2)

  def add(x: Long): Unit = {
    val y = t + x
    if ((~(x ^ t) & (x ^ y)) >= 0L) {
      t = y
    } else {
      ts.append(BigDecimal(t))
      t = x
    }
  }
  def total(): BigDecimal = ts.sum + t
}

trait ReductionLibModule[M[+_]] extends ColumnarTableLibModule[M] {
  trait ReductionLib extends ColumnarTableLib {
    import BigDecimalOperations._
    val ReductionNamespace = Vector()

    override def _libReduction = super._libReduction ++ Set(Count, Max, Min, Sum, Mean, GeometricMean, SumSq, Variance, StdDev, Forall, Exists)

    val CountMonoid = implicitly[Monoid[Count.Result]]
    object Count extends Reduction(ReductionNamespace, "count") {
      // limiting ourselves to 9.2e18 rows doesn't seem like a problem.
      type Result = Long
      
      implicit val monoid = CountMonoid

      val tpe = UnaryOperationType(JType.JUnfixedT, JNumberT)
      
      def reducer(ctx: EvaluationContext): Reducer[Result] = new CReducer[Result] {
        def reduce(cols: JType => Set[Column], range: Range) = {
          val cx = cols(JType.JUnfixedT).toArray
          var count = 0L
          RangeUtil.loop(range) { i =>
            if (Column.isDefinedAt(cx, i)) count += 1L
          }
          count
        }
      }

      def extract(res: Result): Table = Table.constLong(Set(res))

      def extractValue(res: Result) = Some(JNum(res))
    }

    object Max extends Reduction(ReductionNamespace, "max") {
      type Result = Option[BigDecimal]

      implicit val monoid = new Monoid[Result] {
        def zero = None
        def append(left: Result, right: => Result): Result = {
          (for (l <- left; r <- right) yield l max r) orElse left orElse right
        }
      }

      val tpe = UnaryOperationType(JNumberT, JNumberT)
      
      def reducer(ctx: EvaluationContext): Reducer[Result] = new CReducer[Result] {
        def reduce(cols: JType => Set[Column], range: Range): Result = {
          val maxs = cols(JNumberT) map {
            case col: LongColumn =>
              // for longs, we'll use a Boolean to track whether zmax was really
              // seen or not.
              var zmax = Long.MinValue
              val seen = RangeUtil.loopDefined(range, col) { i =>
                val z = col(i)
                if (z > zmax) zmax = z
              }
              if (seen) Some(BigDecimal(zmax)) else None

            case col: DoubleColumn =>
              // since -inf is not a legal value, it's a great starting point for
              // finding the max because any legal value will be greater.
              var zmax = Double.NegativeInfinity
              val seen = RangeUtil.loopDefined(range, col) { i =>
                val z = col(i)
                if (z > zmax) zmax = z
              }
              if (zmax > Double.NegativeInfinity) Some(BigDecimal(zmax)) else None

            case col: NumColumn =>
              // we can just use a null BigDecimal to signal that we haven't
              // found a value yet.
              var zmax: BigDecimal = null
              RangeUtil.loopDefined(range, col) { i =>
                val z = col(i)
                if (zmax == null || z > zmax) zmax = z
              }
              if (zmax != null) Some(zmax) else None

            case _ => None
          }

          // now we just find the max out of all of our column types
          if (maxs.isEmpty) None else maxs.suml(monoid)
        }
      }

      def extract(res: Result): Table =
        res map { v => Table.constDecimal(Set(v)) } getOrElse Table.empty

      def extractValue(res: Result) = res map { JNum(_) }
    }

    object Min extends Reduction(ReductionNamespace, "min") {
      type Result = Option[BigDecimal]

      implicit val monoid = new Monoid[Result] {
        def zero = None
        def append(left: Result, right: => Result): Result = {
          (for (l <- left; r <- right) yield l min r) orElse left orElse right
        }
      }

      val tpe = UnaryOperationType(JNumberT, JNumberT)
      
      def reducer(ctx: EvaluationContext): Reducer[Result] = new CReducer[Result] {
        def reduce(cols: JType => Set[Column], range: Range): Result = {
          val mins = cols(JNumberT) map {
            case col: LongColumn =>
              // for longs, we'll use a Boolean to track whether zmin was really
              // seen or not.
              var zmin = Long.MaxValue
              val seen = RangeUtil.loopDefined(range, col) { i =>
                val z = col(i)
                if (z < zmin) zmin = z
              }
              if (seen) Some(BigDecimal(zmin)) else None

            case col: DoubleColumn =>
              // since +inf is not a legal value, it's a great starting point for
              // finding the min because any legal value will be less.
              var zmin = Double.PositiveInfinity
              RangeUtil.loopDefined(range, col) { i =>
                val z = col(i)
                if (z < zmin) zmin = z
              }
              if (zmin < Double.PositiveInfinity) Some(BigDecimal(zmin)) else None

            case col: NumColumn =>
              // we can just use a null BigDecimal to signal that we haven't
              // found a value yet.
              var zmin: BigDecimal = null
              RangeUtil.loopDefined(range, col) { i =>
                val z = col(i)
                if (zmin == null || z < zmin) zmin = z
              }
              if (zmin != null) Some(zmin) else None

            case _ => None
          }

          // now we just find the min out of all of our column types
          if (mins.isEmpty) None else mins.suml(monoid)
        }
      }

      def extract(res: Result): Table =
        res map { v => Table.constDecimal(Set(v)) } getOrElse Table.empty

      def extractValue(res: Result) = res map { JNum(_) }
    }

    val SumMonoid = implicitly[Monoid[Sum.Result]]
    object Sum extends Reduction(ReductionNamespace, "sum") {
      type Result = Option[BigDecimal]

      implicit val monoid = SumMonoid

      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def reducer(ctx: EvaluationContext): Reducer[Result] = new CReducer[Result] {
        def reduce(cols: JType => Set[Column], range: Range) = {

          val sum = cols(JNumberT) map {

            case col: LongColumn =>
              val ls = new LongAdder()
              val seen = RangeUtil.loopDefined(range, col) { i => ls.add(col(i)) }
              if (seen) Some(ls.total) else None

            // TODO: exactness + overflow
            case col: DoubleColumn =>
              var t = 0.0
              var seen = RangeUtil.loopDefined(range, col) { i => t += col(i) }
              if (seen) Some(BigDecimal(t)) else None

            case col: NumColumn =>
              var t = BigDecimal(0)
              val seen = RangeUtil.loopDefined(range, col) { i => t += col(i) }
              if (seen) Some(t) else None

            case _ => None
          }

          if (sum.isEmpty) None else sum.suml(monoid)
        }
      }

      def extract(res: Result): Table =
        res map { v => Table.constDecimal(Set(v)) } getOrElse Table.empty

      def extractValue(res: Result) = res map { JNum(_) }
    }

    val MeanMonoid = implicitly[Monoid[Mean.Result]]
    object Mean extends Reduction(ReductionNamespace, "mean") {
      type Result = Option[InitialResult]
      type InitialResult = (BigDecimal, Long) // (sum, count)
      
      implicit val monoid = MeanMonoid
      
      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def reducer(ctx: EvaluationContext): Reducer[Result] = new Reducer[Result] {
        def reduce(cols: JType => Set[Column], range: Range): Result = {
          val results = cols(JNumberT) map {

            case col: LongColumn =>
              val ls = new LongAdder()
              var count = 0L
              RangeUtil.loopDefined(range, col) { i =>
                  ls.add(col(i))
                  count += 1L
              }
              if (count > 0L) Some((ls.total, count)) else None

            case col: DoubleColumn =>
              var count = 0L
              var t = BigDecimal(0)
              RangeUtil.loopDefined(range, col) { i =>
                  t += col(i)
                  count += 1L
              }
              if (count > 0L) Some((t, count)) else None

            case col: NumColumn =>
              var count = 0L
              var t = BigDecimal(0)
              RangeUtil.loopDefined(range, col) { i =>
                  t += col(i)
                  count += 1L
              }
              if (count > 0L) Some((t, count)) else None

            case _ => None
          }

          if (results.isEmpty) None else results.suml(monoid)
        }
      }

      def perform(res: Result) = res map {
        case (sum, count) => sum / count
      }

      def extract(res: Result): Table = perform(res) map {
        case v => Table.constDecimal(Set(v))
      } getOrElse Table.empty

      def extractValue(res: Result) = perform(res) map { JNum(_) }
    }
    
    object GeometricMean extends Reduction(ReductionNamespace, "geometricMean") {
      type Result = Option[InitialResult]
      type InitialResult = (BigDecimal, Long)
      
      implicit val monoid = new Monoid[Result] {
        def zero = None
        def append(left: Result, right: => Result) = {
          val both = for ((l1, l2) <- left; (r1, r2) <- right) yield (l1 * r1, l2 + r2)
          both orElse left orElse right
        }
      }

      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def reducer(ctx: EvaluationContext): Reducer[Result] = new Reducer[Option[(BigDecimal, Long)]] {
        def reduce(cols: JType => Set[Column], range: Range): Result = {
          val results = cols(JNumberT) map {
            case col: LongColumn =>
              var prod = BigDecimal(1)
              var count = 0L
              RangeUtil.loopDefined(range, col) { i =>
                  prod *= col(i)
                  count += 1L
              }
              if (count > 0) Some((prod, count)) else None

            case col: DoubleColumn =>
              var prod = BigDecimal(1)
              var count = 0L
              RangeUtil.loopDefined(range, col) { i =>
                  prod *= col(i)
                  count += 1L
              }
              if (count > 0) Some((prod, count)) else None

            case col: NumColumn =>
              var prod = BigDecimal(1)
              var count = 0L
              RangeUtil.loopDefined(range, col) { i =>
                  prod *= col(i)
                  count += 1L
              }
              if (count > 0) Some((prod, count)) else None

            case _ => None
          }

          if (results.isEmpty) None else results.suml(monoid)
        }
      }

      private def perform(res: Result) = res map {
        case (prod, count) => math.pow(prod.toDouble, 1 / count.toDouble)
      } filter(StdLib.doubleIsDefined)

      def extract(res: Result): Table = perform(res) map {
        v => Table.constDouble(Set(v))
      } getOrElse {
        Table.empty
      }

      def extractValue(res: Result) = perform(res).map(JNum(_))
    }
    
    val SumSqMonoid = implicitly[Monoid[SumSq.Result]]
    object SumSq extends Reduction(ReductionNamespace, "sumSq") {
      type Result = Option[BigDecimal]

      implicit val monoid = SumSqMonoid

      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def reducer(ctx: EvaluationContext): Reducer[Result] = new Reducer[Result] {
        def reduce(cols: JType => Set[Column], range: Range): Result = {
          val result = cols(JNumberT) map {

            case col: LongColumn =>
              val ls = new LongAdder()
              val seen = RangeUtil.loopDefined(range, col) { i =>
                ls.addSquare(col(i))
              }
              if (seen) Some(ls.total) else None

            case col: DoubleColumn =>
              var t = BigDecimal(0)
              val seen = RangeUtil.loopDefined(range, col) { i =>
                t += BigDecimal(col(i)) pow 2
              }
              if (seen) Some(t) else None

            case col: NumColumn =>
              var t = BigDecimal(0)
              val seen = RangeUtil.loopDefined(range, col) { i =>
                t += col(i) pow 2
              }
              if (seen) Some(t) else None

            case _ => None
          }
            
          if (result.isEmpty) None else result.suml(monoid)
        }
      }

      def extract(res: Result): Table =
        res map { v => Table.constDecimal(Set(v)) } getOrElse Table.empty

      def extractValue(res: Result) = res map { JNum(_) }
    }

    class CountSumSumSqReducer extends Reducer[Option[(Long, BigDecimal, BigDecimal)]] {
      def reduce(cols: JType => Set[Column], range: Range):
        Option[(Long, BigDecimal, BigDecimal)] = {
        val result = cols(JNumberT) map {
          case col: LongColumn =>
            var count = 0L
            var sum = new LongAdder()
            var sumsq = new LongAdder()
            val seen = RangeUtil.loopDefined(range, col) { i =>
                val z = col(i)
                count += 1
                sum.add(z)
                sumsq.addSquare(z)
            }

            if (seen) Some((count, sum.total, sumsq.total)) else None

          case col: DoubleColumn =>
            var count = 0L
            var sum = BigDecimal(0)
            var sumsq = BigDecimal(0)
            val seen = RangeUtil.loopDefined(range, col) { i =>
                val z = BigDecimal(col(i))
                count += 1
                sum += z
                sumsq += z pow 2
            }

            if (seen) Some((count, sum, sumsq)) else None

          case col: NumColumn =>
            var count = 0L
            var sum = BigDecimal(0)
            var sumsq = BigDecimal(0)
            val seen = RangeUtil.loopDefined(range, col) { i =>
                val z = col(i)
                count += 1
                sum += z
                sumsq += z pow 2
            }

            if (seen) Some((count, sum, sumsq)) else None

          case _ => None
        }

        if (result.isEmpty) None else result.suml
      }
    }

    val VarianceMonoid = implicitly[Monoid[Variance.Result]]
    object Variance extends Reduction(ReductionNamespace, "variance") {
      type Result = Option[InitialResult]

      type InitialResult = (Long, BigDecimal, BigDecimal) 

      implicit val monoid = VarianceMonoid

      val tpe = UnaryOperationType(JNumberT, JNumberT)
      
      def reducer(ctx: EvaluationContext): Reducer[Result] = new CountSumSumSqReducer()

      def perform(res: Result) = res flatMap {
        case (count, sum, sumsq) if count > 0 =>
          val n = (sumsq - (sum * sum / count)) / count
          Some(n)
        case _ =>
          None
      }

      def extract(res: Result): Table = perform(res) map { v =>
          Table.constDecimal(Set(v))
      } getOrElse Table.empty

      def extractValue(res: Result) = perform(res) map { JNum(_) }
    }
    
    val StdDevMonoid = implicitly[Monoid[StdDev.Result]]
    object StdDev extends Reduction(ReductionNamespace, "stdDev") {
      type Result = Option[InitialResult]
      type InitialResult = (Long, BigDecimal, BigDecimal) // (count, sum, sumsq)
      
      implicit val monoid = StdDevMonoid

      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def reducer(ctx: EvaluationContext): Reducer[Result] = new CountSumSumSqReducer()

      def perform(res: Result) = res flatMap {
        case (count, sum, sumsq) if count > 0 =>
          val n = sqrt(count * sumsq - sum * sum) / count
          Some(n)
        case _ =>
          None
      }

      def extract(res: Result): Table = perform(res) map { v =>
        Table.constDecimal(Set(v))
      } getOrElse Table.empty

      // todo using toDouble is BAD
      def extractValue(res: Result) = perform(res) map { JNum(_) }
    }
    
    object Forall extends Reduction(ReductionNamespace, "forall") {
      type Result = Option[Boolean]
      
      val tpe = UnaryOperationType(JBooleanT, JBooleanT)
      
      implicit val monoid = new Monoid[Option[Boolean]] {
        def zero = None
        
        def append(left: Option[Boolean], right: => Option[Boolean]) = {
          val both = for (l <- left; r <- right) yield l && r
          both orElse left orElse right
        }
      }
      
      def reducer(ctx: EvaluationContext): Reducer[Result] = new CReducer[Result] {
        def reduce(cols: JType => Set[Column], range: Range) = {
          if (range.isEmpty) {
            None
          } else {
            var back = true
            var defined = false
            
            cols(JBooleanT) foreach { c =>
              val bc = c.asInstanceOf[BoolColumn]
              var acc = back
              
              val idef = RangeUtil.loopDefined(range, bc) { i =>
                acc &&= bc(i)
              }
              
              back &&= acc
              
              if (idef) {
                defined = true
              }
            }
            
            if (defined)
              Some(back)
            else
              None
          }
        }
      }

      private val default = true
      private def perform(res: Result) = res getOrElse default

      def extract(res: Result): Table =  Table.constBoolean(Set(perform(res)))

      def extractValue(res: Result) = Some(JBool(perform(res)))
    }
    
    object Exists extends Reduction(ReductionNamespace, "exists") {
      type Result = Option[Boolean]
      
      val tpe = UnaryOperationType(JBooleanT, JBooleanT)
      
      implicit val monoid = new Monoid[Option[Boolean]] {
        def zero = None
        
        def append(left: Option[Boolean], right: => Option[Boolean]) = {
          val both = for (l <- left; r <- right) yield l || r
          both orElse left orElse right
        }
      }
      
      def reducer(ctx: EvaluationContext): Reducer[Result] = new CReducer[Result] {
        def reduce(cols: JType => Set[Column], range: Range) = {
          if (range.isEmpty) {
            None
          } else {
            var back = false
            var defined = false
            
            cols(JBooleanT) foreach { c =>
              val bc = c.asInstanceOf[BoolColumn]
              var acc = back
              
              val idef = RangeUtil.loopDefined(range, bc) { i =>
                acc ||= bc(i)
              }
              
              back ||= acc
              
              if (idef) {
                defined = true
              }
            }
            
            if (defined)
              Some(back)
            else
              None
          }
        }
      }
        
      private val default = false
      private def perform(res: Result) = res getOrElse default

      def extract(res: Result): Table = Table.constBoolean(Set(perform(res)))

      def extractValue(res: Result) = Some(JBool(perform(res)))
    }
  }
}
