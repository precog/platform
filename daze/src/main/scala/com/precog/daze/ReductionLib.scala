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
import Scalaz._
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
  object Count extends Reduction(ReductionNamespace, "count") {
    type Result = BigDecimal

    implicit val monoid = new Monoid[List[Result]] {
      def zero = List(BigDecimal(0))
      def append(left: List[Result], right: => List[Result]): List[Result] = List(left.head + right.head)
    }

    val tpe = UnaryOperationType(JType.JUnfixedT, JNumberT)
    
    def reducer: Reducer[List[Result]] = new CReducer[List[Result]] {
      def reduce(cols: JType => Set[Column], range: Range) = {
        val cx = cols(JType.JUnfixedT)
        val colSeq = range.view filter { i => cx.exists(_.isDefinedAt(i)) }
        List(colSeq.size)
      }
    }

    def extract(res: List[Result]): Table = ops.constDecimal(Set(CNum(res.head)))
  }

  object Max extends Reduction(ReductionNamespace, "max") {
    type Result = Option[BigDecimal]

    val monoid = new Monoid[List[Result]] {
      def zero = List(None)
      def append(left: List[Result], right: => List[Result]): List[Result] = {
        val result = (for (l <- left.head; r <- right.head) yield l max r) orElse left.head orElse right.head
        List(result)
      }
    }

    val tpe = UnaryOperationType(JNumberT, JNumberT)
    
    def reducer: Reducer[List[Result]] = new CReducer[List[Result]] {
      def reduce(cols: JType => Set[Column], range: Range): List[Result] = {
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

        if (max.isEmpty) List(None)
        else List(Some(max.suml))
        
        //(max.isEmpty).option(max.suml)
      }
    }

    def extract(res: List[Result]): Table = {
      val result = res.head
      result map { r => ops.constDecimal(Set(CNum(r))) } getOrElse ops.empty
    }
  }

  object Min extends Reduction(ReductionNamespace, "min") {
    type Result = Option[BigDecimal]

    val monoid = new Monoid[List[Result]] {
      def zero = List(None)
      def append(left: List[Result], right: => List[Result]): List[Result] = {
        val result = (for (l <- left.head; r <- right.head) yield l max r) orElse left.head orElse right.head
        List(result)
      }
    }

    val tpe = UnaryOperationType(JNumberT, JNumberT)
    
    def reducer: Reducer[List[Result]] = new CReducer[List[Result]] {
      def reduce(cols: JType => Set[Column], range: Range): List[Result] = {
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

        if (min.isEmpty) List(None)
        else List(Some(min.suml))
      } 
    }

    def extract(res: List[Result]): Table = {
      val result = res.head
      result map { r => ops.constDecimal(Set(CNum(r))) } getOrElse ops.empty
    }
  }

  object Sum extends Reduction(ReductionNamespace, "sum") {
    type Result = Option[BigDecimal]

    implicit val monoid = new Monoid[List[Result]] {
      def zero = List(None)
      def append(left: List[Result], right: => List[Result]): List[Result] = {
        val result = (for (l <- left.head; r <- right.head) yield l + r) orElse left.head orElse right.head
        List(result)
      }
    }

    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def reducer: Reducer[List[Result]] = new CReducer[List[Result]] {
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

        if (sum.isEmpty) List(None)
        else List(Some(sum.suml))
      }
    }

    def extract(res: List[Result]): Table = {
      val result = res.head
      result map { r => ops.constDecimal(Set(CNum(r))) } getOrElse ops.empty
    }
  }
  
  object Mean extends Reduction(ReductionNamespace, "mean") {
    type Result = Option[InitialResult]
    type InitialResult = (BigDecimal, BigDecimal)   // (sum, count)
    
    implicit val monoid = new Monoid[List[Result]] {
      def zero = List(None)
      def append(left: List[Result], right: => List[Result]): List[Result] = {
        val result = (for (l <- left.head; r <- right.head) yield l /*|+| r*/) orElse left.head orElse right.head
        List(result)
      }
    }
    
    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def reducer: Reducer[List[Result]] = new Reducer[List[Result]] {
      def reduce(cols: JType => Set[Column], range: Range): List[Result] = {
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

        if (result.isEmpty) List(None)
        else List(Some(result.suml))
      }
    }

    def extract(res: List[Result]): Table = {
      val filteredResult = res.head filter { case (_, count) => count != 0 } 
      filteredResult map { case (sum, count) => ops.constDecimal(Set(CNum(sum / count))) } getOrElse ops.empty
    }
  }
  
  object GeometricMean extends Reduction(ReductionNamespace, "geometricMean") {
    type Result = Option[InitialResult]
    type InitialResult = (BigDecimal, BigDecimal)
    
    implicit val monoid = new Monoid[List[Result]] {    //(product, count)
      def zero = List(None)
      def append(left: List[Result], right: => List[Result]): List[Result] = {
        val both = for ((l1, l2) <- left.head; (r1, r2) <- right.head) yield (l1 * r1, l2 + r2)
        List(both orElse left.head orElse right.head)
      }
    }

    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def reducer: Reducer[List[Result]] = new Reducer[List[Result]] {
      def reduce(cols: JType => Set[Column], range: Range): List[Result] = {
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

        if (result.isEmpty) List(None)
        else List(Some(result.suml))
      }
    }

    def extract(res: List[Result]): Table = { //TODO division by zero 
      val filteredResult = res.head filter { case (_, count) => count != 0 }
      filteredResult map { case (prod, count) => ops.constDecimal(Set(CNum(math.pow(prod.toDouble, 1 / count.toDouble)))) } getOrElse ops.empty
    }
  }
  
  object SumSq extends Reduction(ReductionNamespace, "sumSq") {
    type Result = Option[BigDecimal]

    implicit val monoid = Sum.monoid

    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def reducer: Reducer[List[Result]] = new Reducer[List[Result]] {
      def reduce(cols: JType => Set[Column], range: Range): List[Result] = {
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
          
        if (result.isEmpty) List(None)
        else List(Some(result.suml))
      }
    }

    def extract(res: List[Result]): Table = {
      val result = res.head
      res.head map { r => ops.constDecimal(Set(CNum(r))) } getOrElse ops.empty
    }
  }
  
  //val VarianceMonoid = implicitly[Monoid[Variance.Result]]
  object Variance extends Reduction(ReductionNamespace, "variance") {
    type Result = Option[InitialResult]
    type InitialResult = (BigDecimal, BigDecimal, BigDecimal)   // (count, sum, sumsq)

    implicit val monoid = new Monoid[List[Result]] {
      def zero = List(None)
      def append(left: List[Result], right: => List[Result]): List[Result] = {
        val result = (for (l <- left.head; r <- right.head) yield l /*|+| r*/) orElse left.head orElse right.head
        List(result)
      }
    }

    val tpe = UnaryOperationType(JNumberT, JNumberT)
    
    def reducer: Reducer[List[Result]] = new Reducer[List[Result]] {
      def reduce(cols: JType => Set[Column], range: Range): List[Result] = {
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

        if (result.isEmpty) List(None)
        else List(Some(result.suml))
      }
    }

    def extract(res: List[Result]): Table = {
      val filteredResult = res.head filter { case (count, _, _) => count != 0 }
      filteredResult map { case (count, sum, sumsq) => ops.constDecimal(Set(CNum((sumsq - (sum * (sum / count))) / count))) } getOrElse ops.empty  //todo using toDouble is BAD
    }
  }
  
  object StdDev extends Reduction(ReductionNamespace, "stdDev") {
    type Result = Option[InitialResult]
    type InitialResult = (BigDecimal, BigDecimal, BigDecimal)   // (count, sum, sumsq)
    
    implicit val monoid = new Monoid[List[Result]] {
      def zero = List(None)
      def append(left: List[Result], right: => List[Result]): List[Result] = {
        val result = (for (l <- left.head; r <- right.head) yield l /*|+| r*/) orElse left.head orElse right.head
        List(result)
      }
    }

    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def reducer: Reducer[List[Result]] = new Reducer[List[Result]] {
      def reduce(cols: JType => Set[Column], range: Range): List[Result] = {
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

        if (result.isEmpty) List(None)
        else List(Some(result.suml))
      }
    }

    def extract(res: List[Result]): Table = {
      val filteredResult = res.head filter { case (count, _, _) => count != 0 }
      filteredResult map { case (count, sum, sumsq) => ops.constDecimal(Set(CNum(sqrt(count * sumsq - sum * sum) / count))) } getOrElse ops.empty  //todo using toDouble is BAD
    }
  }
}
