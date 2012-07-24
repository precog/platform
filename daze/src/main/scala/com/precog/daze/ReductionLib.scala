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

trait ReductionLib[M[+_]] extends GenOpcode[M] with BigDecimalOperations with Evaluator[M] {  
  val ReductionNamespace = Vector()

  override def _libReduction = super._libReduction ++ Set(Count, Max, Min, Sum, Mean, GeometricMean, SumSq, Variance, StdDev)

  // TODO swap to Reduction
  val CountMonoid = implicitly[Monoid[Count.Result]]
  object Count extends Reduction(ReductionNamespace, "count") {
    type Result = BigDecimal
    
    implicit val monoid = CountMonoid

    val tpe = UnaryOperationType(JType.JUnfixedT, JNumberT)
    
    def reducer: Reducer[Result] = new CReducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range) = {
        val cx = cols(JType.JUnfixedT)
        val colSeq = range.view filter { i => cx.exists(_.isDefinedAt(i)) }
        colSeq.size
      }
    }

    def extract(res: Result): Table = ops.constDecimal(Set(CNum(res)))
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
        val max = cols(JNumberT) flatMap {
          case col: LongColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty)
              None
            else
              Some(mapped.max: BigDecimal)
          case col: DoubleColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty)
              None
            else
              Some(mapped.max: BigDecimal)
          case col: NumColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty)
              None
            else
              Some(mapped.max: BigDecimal)

          case _ => None
        } 

        if (max.isEmpty) None
        else Some(max.suml)
        
        //(max.isEmpty).option(max.suml)
      }
    }

    def extract(res: Result): Table =
      res map { r => ops.constDecimal(Set(CNum(r))) } getOrElse ops.empty
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
        val min = cols(JType.JUnfixedT) flatMap {
          case col: LongColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty)
              None
            else
              Some(BigDecimal(mapped.min))
          case col: DoubleColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) None
            else Some(BigDecimal(mapped.min))

          case col: NumColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) None 
            else Some(mapped.min: BigDecimal)

          case _ => None
        } 

        if (min.isEmpty) None
        else Some(min.suml)
      } 
    }

    def extract(res: Result): Table =
      res map { r => ops.constDecimal(Set(CNum(r))) } getOrElse ops.empty
  }

  val SumMonoid = implicitly[Monoid[Sum.Result]]
  object Sum extends Reduction(ReductionNamespace, "sum") {
    type Result = Option[BigDecimal]

    implicit val monoid = SumMonoid

    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def reducer: Reducer[Result] = new CReducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range) = { 

        val sum = cols(JNumberT) flatMap {
          case col: LongColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty)
              None
            else
              Some(BigDecimal(mapped.sum))
          case col: DoubleColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty)
              None
            else
              Some(BigDecimal(mapped.sum))
          case col: NumColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty)
              None
            else
              Some(mapped.sum)

          case _ => None
        } 

        if (sum.isEmpty) None
        else Some(sum.suml)   

        //(sum.isEmpty).option(sum.suml)
      }
    }

    def extract(res: Result): Table =
      res map { r => ops.constDecimal(Set(CNum(r))) } getOrElse ops.empty
  }
  
  val MeanMonoid = implicitly[Monoid[Mean.Result]]
  object Mean extends Reduction(ReductionNamespace, "mean") {
    type Result = Option[InitialResult]
    type InitialResult = (BigDecimal, BigDecimal)   // (sum, count)
    
    implicit val monoid = MeanMonoid
    
    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def reducer: Reducer[Result] = new Reducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val result = cols(JNumberT) flatMap {
          case col: LongColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0))) {
                case ((sum, count), value) => (sum + value: BigDecimal, count + 1: BigDecimal)
              }

              Some(foldedMapped)
            }
          case col: DoubleColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0))) {
                case ((sum, count), value) => (sum + value: BigDecimal, count + 1: BigDecimal)
              }

              Some(foldedMapped)
            }
          case col: NumColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0))) {
                case ((sum, count), value) => (sum + value: BigDecimal, count + 1: BigDecimal)
              }

              Some(foldedMapped)
            }

          case _ => None
        } 

        if (result.isEmpty) None
        else Some(result.suml)
      }
    }

    def extract(res: Result): Table = {
      val filteredResult = res filter { case (_, count) => count != 0 } 
      filteredResult map { case (sum, count) => ops.constDecimal(Set(CNum(sum / count))) } getOrElse ops.empty
    }
  }
  
  object GeometricMean extends Reduction(ReductionNamespace, "geometricMean") {
    type Result = Option[InitialResult]
    type InitialResult = (BigDecimal, BigDecimal)
    
    implicit val monoid = new Monoid[Result] {    //(product, count)
      def zero = None
      def append(left: Result, right: => Result) = {
        val both = for ((l1, l2) <- left; (r1, r2) <- right) yield (l1 * r1, l2 + r2)
        both orElse left orElse right
      }
    }

    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def reducer: Reducer[Result] = new Reducer[Option[(BigDecimal, BigDecimal)]] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val result = cols(JNumberT) flatMap {
          case col: LongColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(1), BigDecimal(0))) {
                case ((prod, count), value) => (prod * value: BigDecimal, count + 1: BigDecimal)
              }

              Some(foldedMapped)
            }
          case col: DoubleColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(1), BigDecimal(0))) {
                case ((prod, count), value) => (prod * value: BigDecimal, count + 1: BigDecimal)
              }

              Some(foldedMapped)
            }
          case col: NumColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(1), BigDecimal(0))) {
                case ((prod, count), value) => (prod * value: BigDecimal, count + 1: BigDecimal)
              }

              Some(foldedMapped)
            }

          case _ => None
        }

        if (result.isEmpty) None
        else Some(result.suml)
      }
    }

    def extract(res: Result): Table = { //TODO division by zero 
      val filteredResult = res filter { case (_, count) => count != 0 }
      filteredResult map { case (prod, count) => ops.constDecimal(Set(CNum(math.pow(prod.toDouble, 1 / count.toDouble)))) } getOrElse ops.empty
    }
  }
  
  val SumSqMonoid = implicitly[Monoid[SumSq.Result]]
  object SumSq extends Reduction(ReductionNamespace, "sumSq") {
    type Result = Option[BigDecimal]

    implicit val monoid = SumSqMonoid

    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def reducer: Reducer[Result] = new Reducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val result = cols(JNumberT) flatMap {
          case col: LongColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: BigDecimal = mapped.foldLeft(BigDecimal(0)) {
                case (sumsq, value) => (sumsq + (value * value): BigDecimal)
              }

              Some(foldedMapped)
            }
          case col: DoubleColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: BigDecimal = mapped.foldLeft(BigDecimal(0)) {
                case (sumsq, value) => (sumsq + (value * value): BigDecimal)
              }

              Some(foldedMapped)
            }
          case col: NumColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: BigDecimal = mapped.foldLeft(BigDecimal(0)) {
                case (sumsq, value) => (sumsq + (value * value): BigDecimal)
              }

              Some(foldedMapped)
            }

          case _ => None
        }
          
        if (result.isEmpty) None
        else Some(result.suml)
      }
    }

    def extract(res: Result): Table =
      res map { r => ops.constDecimal(Set(CNum(r))) } getOrElse ops.empty
  }
  
  val VarianceMonoid = implicitly[Monoid[Variance.Result]]
  object Variance extends Reduction(ReductionNamespace, "variance") {
    type Result = Option[InitialResult]
    type InitialResult = (BigDecimal, BigDecimal, BigDecimal)   // (count, sum, sumsq)

    implicit val monoid = VarianceMonoid

    val tpe = UnaryOperationType(JNumberT, JNumberT)
    
    def reducer: Reducer[Result] = new Reducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val result = cols(JNumberT) flatMap {
          case col: LongColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                case ((count, sum, sumsq), value) => (count + 1: BigDecimal, sum + value: BigDecimal, sumsq + (value * value))
              }

              Some(foldedMapped)
            }
          case col: DoubleColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                case ((count, sum, sumsq), value) => (count + 1: BigDecimal, sum + value: BigDecimal, sumsq + (value * value))
              }

              Some(foldedMapped)
            }
          case col: NumColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                case ((count, sum, sumsq), value) => (count + 1: BigDecimal, sum + value: BigDecimal, sumsq + (value * value))
              }

              Some(foldedMapped)
            }
          case _ => None
        }

        if (result.isEmpty) None
        else Some(result.suml)
      }
    }

    def extract(res: Result): Table = {
      val filteredResult = res filter { case (count, _, _) => count != 0 }
      filteredResult map { case (count, sum, sumsq) => ops.constDecimal(Set(CNum((sumsq - (sum * (sum / count))) / count))) } getOrElse ops.empty  //todo using toDouble is BAD
    }
  }
  
  val StdDevMonoid = implicitly[Monoid[StdDev.Result]]
  object StdDev extends Reduction(ReductionNamespace, "stdDev") {
    type Result = Option[InitialResult]
    type InitialResult = (BigDecimal, BigDecimal, BigDecimal)   // (count, sum, sumsq)
    
    implicit val monoid = StdDevMonoid

    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def reducer: Reducer[Result] = new Reducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val result = cols(JNumberT) flatMap {
          case col: LongColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                case ((count, sum, sumsq), value) => (count + 1: BigDecimal, sum + value: BigDecimal, sumsq + (value * value))
              }

              Some(foldedMapped)
            }
          case col: DoubleColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                case ((count, sum, sumsq), value) => (count + 1: BigDecimal, sum + value: BigDecimal, sumsq + (value * value))
              }

              Some(foldedMapped)
            }
          case col: NumColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                case ((count, sum, sumsq), value) => (count + 1: BigDecimal, sum + value: BigDecimal, sumsq + (value * value))
              }

              Some(foldedMapped)
            }
          case _ => None
        }

        if (result.isEmpty) None
        else Some(result.suml)
      }
    }

    def extract(res: Result): Table = {
      val filteredResult = res filter { case (count, _, _) => count != 0 }
      filteredResult map { case (count, sum, sumsq) => ops.constDecimal(Set(CNum(sqrt(count * sumsq - sum * sum) / count))) } getOrElse ops.empty  //todo using toDouble is BAD
    }
  }
}
