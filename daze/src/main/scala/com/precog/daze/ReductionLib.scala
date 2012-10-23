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

import yggdrasil._
import yggdrasil.table._

import com.precog.util._

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
  def loop(r: Range, f: Int => Unit) {
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
  def loopDefined(r: Range, col: Column, f: Int => Unit): Boolean = {
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

trait ReductionLib[M[+_]] extends GenOpcode[M] with BigDecimalOperations with Evaluator[M] {
  val ReductionNamespace = Vector()

  override def _libReduction = super._libReduction ++ Set(Count, Max, Min, Sum, Mean, GeometricMean, SumSq, Variance, StdDev)

  // TODO swap to Reduction
  val CountMonoid = implicitly[Monoid[Count.Result]]
  object Count extends Reduction(ReductionNamespace, "count") {
    // limiting ourselves to 9.2e18 rows doesn't seem like a problem.
    type Result = Long
    
    implicit val monoid = CountMonoid

    val tpe = UnaryOperationType(JType.JUnfixedT, JNumberT)
    
    def reducer: Reducer[Result] = new CReducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range) = {
        val cx = cols(JType.JUnfixedT).toArray
        var count = 0L
        RangeUtil.loop(range, { i =>
          if (Column.isDefinedAt(cx, i)) count += 1L
        })
        count
      }
    }

    def extract(res: Result): Table = Table.constDecimal(Set(CNum(res)))
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
    
    def reducer: Reducer[Result] = new CReducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val maxs = cols(JNumberT) map {
          case col: LongColumn =>
            // for longs, we'll use a Boolean to track whether zmax was really
            // seen or not.
            var zmax = Long.MinValue
            val seen = RangeUtil.loopDefined(range, col, { i =>
              val z = col(i)
              if (z > zmax) zmax = z
            })
            if (seen) Some(BigDecimal(zmax)) else None

          case col: DoubleColumn =>
            // since -inf is not a legal value, it's a great starting point for
            // finding the max because any legal value will be greater.
            var zmax = Double.NegativeInfinity
            val seen = RangeUtil.loopDefined(range, col, { i =>
              val z = col(i)
              if (z > zmax) zmax = z
            })
            if (zmax > Double.NegativeInfinity) Some(BigDecimal(zmax)) else None

          case col: NumColumn =>
            // we can just use a null BigDecimal to signal that we haven't
            // found a value yet.
            var zmax: BigDecimal = null
            RangeUtil.loopDefined(range, col, { i =>
              val z = col(i)
              if (zmax == null || z > zmax) zmax = z
            })
            if (zmax != null) Some(zmax) else None

          case _ => None
        }

        // now we just find the max out of all of our column types
        if (maxs.isEmpty) None else maxs.suml(monoid)
      }
    }

    def extract(res: Result): Table =
      res map { r => Table.constDecimal(Set(CNum(r))) } getOrElse Table.empty
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
    
    def reducer: Reducer[Result] = new CReducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val mins = cols(JNumberT) map {
          case col: LongColumn =>
            // for longs, we'll use a Boolean to track whether zmin was really
            // seen or not.
            var zmin = Long.MaxValue
            val seen = RangeUtil.loopDefined(range, col, { i =>
              val z = col(i)
              if (z < zmin) zmin = z
            })
            if (seen) Some(BigDecimal(zmin)) else None

          case col: DoubleColumn =>
            // since +inf is not a legal value, it's a great starting point for
            // finding the min because any legal value will be less.
            var zmin = Double.PositiveInfinity
            RangeUtil.loopDefined(range, col, { i =>
              val z = col(i)
              if (z < zmin) zmin = z
            })
            if (zmin < Double.PositiveInfinity) Some(BigDecimal(zmin)) else None

          case col: NumColumn =>
            // we can just use a null BigDecimal to signal that we haven't
            // found a value yet.
            var zmin: BigDecimal = null
            RangeUtil.loopDefined(range, col, { i =>
              val z = col(i)
              if (zmin == null || z < zmin) zmin = z
            })
            if (zmin != null) Some(zmin) else None

          case _ => None
        }

        // now we just find the min out of all of our column types
        if (mins.isEmpty) None else mins.suml(monoid)
      }
    }

    def extract(res: Result): Table =
      res map { r => Table.constDecimal(Set(CNum(r))) } getOrElse Table.empty
  }

  val SumMonoid = implicitly[Monoid[Sum.Result]]
  object Sum extends Reduction(ReductionNamespace, "sum") {
    type Result = Option[BigDecimal]

    implicit val monoid = SumMonoid

    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def reducer: Reducer[Result] = new CReducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range) = {

        val sum = cols(JNumberT) map {

          case col: LongColumn =>
            val ls = new LongAdder()
            val seen = RangeUtil.loopDefined(range, col, i => ls.add(col(i)))
            if (seen) Some(ls.total) else None

          // TODO: exactness + overflow
          case col: DoubleColumn =>
            var t = 0.0
            var seen = RangeUtil.loopDefined(range, col, i => t += col(i))
            if (seen) Some(BigDecimal(t)) else None

          case col: NumColumn =>
            var t = BigDecimal(0)
            val seen = RangeUtil.loopDefined(range, col, i => t += col(i))
            if (seen) Some(t) else None

          case _ => None
        }

        if (sum.isEmpty) None else sum.suml(monoid)
      }
    }

    def extract(res: Result): Table =
      res map { r => Table.constDecimal(Set(CNum(r))) } getOrElse Table.empty
  }

  val MeanMonoid = implicitly[Monoid[Mean.Result]]
  object Mean extends Reduction(ReductionNamespace, "mean") {
    type Result = Option[InitialResult]
    type InitialResult = (BigDecimal, Long) // (sum, count)
    
    implicit val monoid = MeanMonoid
    
    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def reducer: Reducer[Result] = new Reducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val results = cols(JNumberT) map {

          case col: LongColumn =>
            val ls = new LongAdder()
            var count = 0L
            RangeUtil.loopDefined(range, col, { i =>
                ls.add(col(i))
                count += 1L
            })
            if (count > 0L) Some((ls.total, count)) else None

          case col: DoubleColumn =>
            var count = 0L
            var t = BigDecimal(0)
            RangeUtil.loopDefined(range, col, { i =>
                t += col(i)
                count += 1L
            })
            if (count > 0L) Some((t, count)) else None

          case col: NumColumn =>
            var count = 0L
            var t = BigDecimal(0)
            RangeUtil.loopDefined(range, col, { i =>
                t += col(i)
                count += 1L
            })
            if (count > 0L) Some((t, count)) else None

          case _ => None
        }

        if (results.isEmpty) None else results.suml(monoid)
      }
    }

    def extract(res: Result): Table = res map {
      case (sum, count) => Table.constDecimal(Set(CNum(sum / count)))
    } getOrElse Table.empty
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

    def reducer: Reducer[Result] = new Reducer[Option[(BigDecimal, Long)]] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val results = cols(JNumberT) map {
          case col: LongColumn =>
            var prod = BigDecimal(1)
            var count = 0L
            RangeUtil.loopDefined(range, col, { i =>
                prod *= col(i)
                count += 1L
            })
            if (count > 0) Some((prod, count)) else None

          case col: DoubleColumn =>
            var prod = BigDecimal(1)
            var count = 0L
            RangeUtil.loopDefined(range, col, { i =>
                prod *= col(i)
                count += 1L
            })
            if (count > 0) Some((prod, count)) else None

          case col: NumColumn =>
            var prod = BigDecimal(1)
            var count = 0L
            RangeUtil.loopDefined(range, col, { i =>
                prod *= col(i)
                count += 1L
            })
            if (count > 0) Some((prod, count)) else None

          case _ => None
        }

        if (results.isEmpty) None else results.suml(monoid)
      }
    }

    def extract(res: Result): Table = res map {
      case (prod, count) => math.pow(prod.toDouble, 1 / count.toDouble)
    } filter(StdLib.doubleIsDefined) map {
      mean => Table.constDecimal(Set(CNum(mean)))
    } getOrElse {
      Table.empty
    }
  }
  
  val SumSqMonoid = implicitly[Monoid[SumSq.Result]]
  object SumSq extends Reduction(ReductionNamespace, "sumSq") {
    type Result = Option[BigDecimal]

    implicit val monoid = SumSqMonoid

    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def reducer: Reducer[Result] = new Reducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val result = cols(JNumberT) map {

          case col: LongColumn =>
            val ls = new LongAdder()
            val seen = RangeUtil.loopDefined(range, col, { i =>
              ls.addSquare(col(i))
            })
            if (seen) Some(ls.total) else None

          case col: DoubleColumn =>
            var t = BigDecimal(0)
            val seen = RangeUtil.loopDefined(range, col, { i =>
              t += BigDecimal(col(i)) pow 2
            })
            if (seen) Some(t) else None

          case col: NumColumn =>
            var t = BigDecimal(0)
            val seen = RangeUtil.loopDefined(range, col, { i =>
              t += col(i) pow 2
            })
            if (seen) Some(t) else None

          case _ => None
        }
          
        if (result.isEmpty) None else result.suml(monoid)
      }
    }

    def extract(res: Result): Table =
      res map { r => Table.constDecimal(Set(CNum(r))) } getOrElse Table.empty
  }

  class CountSumSumSqReducer extends Reducer[Option[(Long, BigDecimal, BigDecimal)]] {
    def reduce(cols: JType => Set[Column], range: Range):
      Option[(Long, BigDecimal, BigDecimal)] = {
      val result = cols(JNumberT) map {
        case col: LongColumn =>
          var count = 0L
          var sum = new LongAdder()
          var sumsq = new LongAdder()
          val seen = RangeUtil.loopDefined(range, col, { i =>
              val z = col(i)
              count += 1
              sum.add(z)
              sumsq.addSquare(z)
          })

          if (seen) Some((count, sum.total, sumsq.total)) else None

        case col: DoubleColumn =>
          var count = 0L
          var sum = BigDecimal(0)
          var sumsq = BigDecimal(0)
          val seen = RangeUtil.loopDefined(range, col, { i =>
              val z = BigDecimal(col(i))
              count += 1
              sum += z
              sumsq += z pow 2
          })

          if (seen) Some((count, sum, sumsq)) else None

        case col: NumColumn =>
          var count = 0L
          var sum = BigDecimal(0)
          var sumsq = BigDecimal(0)
          val seen = RangeUtil.loopDefined(range, col, { i =>
              val z = col(i)
              count += 1
              sum += z
              sumsq += z pow 2
          })

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
    
    def reducer: Reducer[Result] = new CountSumSumSqReducer()

    // todo using toDouble is BAD
    def extract(res: Result): Table = res map {
      case (count, sum, sumsq) if count > 0 =>
        val n = (sumsq - (sum * sum / count)) / count
        Table.constDecimal(Set(CNum(n)))
    } getOrElse Table.empty
  }
  
  val StdDevMonoid = implicitly[Monoid[StdDev.Result]]
  object StdDev extends Reduction(ReductionNamespace, "stdDev") {
    type Result = Option[InitialResult]
    type InitialResult = (Long, BigDecimal, BigDecimal) // (count, sum, sumsq)
    
    implicit val monoid = StdDevMonoid

    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def reducer: Reducer[Result] = new CountSumSumSqReducer()

    // todo using toDouble is BAD
    def extract(res: Result): Table = res map {
      case (count, sum, sumsq) if count > 0 =>
        val n = sqrt(count * sumsq - sum * sum) / count
        Table.constDecimal(Set(CNum(n)))
    } getOrElse Table.empty 
  }
}
