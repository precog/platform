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

trait ReductionLib extends GenOpcode with ImplLibrary with BigDecimalOperations with Evaluator {  
  val ReductionNamespace = Vector()

  override def _libReduction = super._libReduction ++ Set(Count, Max, Min, Sum, Mean, GeometricMean, SumSq, Variance, StdDev)

  // TODO swap to Reduction
  object Count extends Reduction(ReductionNamespace, "count") {
    type Result = Long
    
    implicit def monoid = implicitly[Monoid[Result]]
    
    def reducer: Reducer[Result] = new CReducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range) = {
        val cx = cols(JType.JUnfixedT)
        val colSeq = range.view filter { i => cx.exists(_.isDefinedAt(i)) }
        colSeq.size
      }
    }

    def extract(res: Result): Table = ops.constLong(Set(CLong(res)))
  }

  object Max extends Reduction(ReductionNamespace, "max") {
    type Result = Option[BigDecimal]

    implicit def monoid = implicitly[Monoid[Result]]
    
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
        
        (max.isEmpty).option(max.suml)
      }
    }

    def extract(res: Result): Table =
      res map { r => ops.constDecimal(Set(CNum(r))) } getOrElse ops.empty
  }

  object Min extends Reduction(ReductionNamespace, "min") {
    type Result = Option[BigDecimal]

    implicit def monoid = implicitly[Monoid[Result]]
    
    def reducer: Reducer[Result] = new CReducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val min = cols(JNumberT) flatMap {
          case col: LongColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty)
              None
            else
              Some(mapped.min: BigDecimal)
          case col: DoubleColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            (mapped.isEmpty).option(mapped.min: BigDecimal)

          case col: NumColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            (mapped.isEmpty).option(mapped.min: BigDecimal)

          case _ => None
        } 
        
        (min.isEmpty).option(min.suml)
      } 
    }

    def extract(res: Result): Table =
      res map { r => ops.constDecimal(Set(CNum(r))) } getOrElse ops.empty
  }
  
  object Sum extends Reduction(ReductionNamespace, "sum") {
    type Result = Option[BigDecimal]

    implicit def monoid = implicitly[Monoid[Result]]

    def reducer: Reducer[Result] = new CReducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range) = {
        val sum = cols(JNumberT) flatMap {
          case col: LongColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty)
              None
            else
              Some(mapped.sum: BigDecimal)
          case col: DoubleColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty)
              None
            else
              Some(mapped.sum: BigDecimal)
          case col: NumColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty)
              None
            else
              Some(mapped.sum: BigDecimal)

          case _ => None
        } 

        (sum.isEmpty).option(sum.suml)
      }
    }

    def extract(res: Result): Table =
      res map { r => ops.constDecimal(Set(CNum(r))) } getOrElse ops.empty
  }
  
  object Mean extends Reduction(ReductionNamespace, "mean") {
    type Result = Option[InitialResult]
    type InitialResult = (BigDecimal, BigDecimal)   // (sum, count)
    
    implicit def monoid = implicitly[Monoid[Result]]
    
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

        (result.isEmpty).option(result.suml)
      }
    }

    def extract(res: Result): Table =
      res map { case (sum, count) => ops.constDecimal(Set(CNum(sum / count))) } getOrElse ops.empty
  }
  
  object GeometricMean extends Reduction(ReductionNamespace, "geometricMean") {
    type Result = Option[InitialResult]
    type InitialResult = (BigDecimal, BigDecimal)
    
    implicit def monoid = new Monoid[Result] {    //(product, count)
      def zero = None
      def append(left: Result, right: => Result) = {
        val both = for ((l1, l2) <- left; (r1, r2) <- right) yield (l1 * r1, l2 + r2)
        both orElse left orElse right
      }
    }

    def reducer: Reducer[Result] = new Reducer[Option[(BigDecimal, BigDecimal)]] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val result = cols(JNumberT) flatMap {
          case col: LongColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0))) {
                case ((prod, count), value) => (prod * value: BigDecimal, count + 1: BigDecimal)
              }

              Some(foldedMapped)
            }
          case col: DoubleColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0))) {
                case ((prod, count), value) => (prod * value: BigDecimal, count + 1: BigDecimal)
              }

              Some(foldedMapped)
            }
          case col: NumColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0))) {
                case ((prod, count), value) => (prod * value: BigDecimal, count + 1: BigDecimal)
              }

              Some(foldedMapped)
            }

          case _ => None
        }

        (result.isEmpty).option(result.suml)
      }
    }

    def extract(res: Result): Table = { //TODO division by zero 
      val nonzeroRes = res filter { case (_, count) => count != 0 }

      nonzeroRes map { case (prod, count) => ops.constDecimal(Set(CNum(math.pow(prod.toDouble, 1 / count.toDouble)))) } getOrElse ops.empty
    }
  }
  
  object SumSq extends Reduction(ReductionNamespace, "sumSq") {
    type Result = Option[BigDecimal]

    implicit def monoid = implicitly[Monoid[Result]]
    
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
          
        (result.isEmpty).option(result.suml)
      }
    }

    def extract(res: Result): Table =
      res map { r => ops.constDecimal(Set(CNum(r))) } getOrElse ops.empty
  }
  
  object Variance extends Reduction(ReductionNamespace, "variance") {
    type Result = Option[InitialResult]
    type InitialResult = (BigDecimal, BigDecimal, BigDecimal)   // (count, sum, sumsq)

    implicit def monoid = implicitly[Monoid[Result]]
    
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

        (result.isEmpty).option(result.suml)
      }
    }

    def extract(res: Result): Table =
      res map { case (count, sum, sumsq) => ops.constDecimal(Set(CNum((sumsq - (sum * (sum / count))) / count))) } getOrElse ops.empty  //todo using toDouble is BAD
  }
  
  object StdDev extends Reduction(ReductionNamespace, "stdDev") {
    type Result = Option[InitialResult]
    type InitialResult = (BigDecimal, BigDecimal, BigDecimal)   // (count, sum, sumsq)
    
    implicit def monoid = implicitly[Monoid[Result]]

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

        (result.isEmpty).option(result.suml)
      }
    }

    def extract(res: Result): Table =
      res map { case (count, sum, sumsq) => ops.constDecimal(Set(CNum(sqrt(count * sumsq - sum * sum) / count))) } getOrElse ops.empty  //todo using toDouble is BAD
  }
}
