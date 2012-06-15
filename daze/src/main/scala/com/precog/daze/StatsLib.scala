package com.precog
package daze

import bytecode.Library
import bytecode.BuiltInFunc1
import bytecode.BuiltInFunc2

import yggdrasil._

import com.precog.util.IdGen

trait StatsLib extends GenOpcode with ImplLibrary with DatasetOpsComponent with BigDecimalOperations with Evaluator {
  val StatsNamespace = Vector("std", "stats")

  override def _lib1 = super._lib1 ++ Set(DenseRank, Rank)
  override def _lib2 = super._lib2 ++ Set(Covariance, LinearCorrelation, LinearRegression, LogarithmicRegression)

  import yggConfig._

  object LinearCorrelation extends BIF2(StatsNamespace, "corr") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = { 
      case (SDecimal(num1), SDecimal(num2)) => SArray(Vector(SDecimal(num1), SDecimal(num2)))  
    }

    override val requiresReduction = true

    override def reduced(enum: Dataset[SValue]): Option[SValue] = {              
      val (count, sum1, sum2, sumsq1, sumsq2, productSum) = enum.reduce((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
        case ((count, sum1, sum2, sumsq1, sumsq2, productSum), SArray(Vector(SDecimal(num1), SDecimal(num2)))) => {
          (count + 1, sum1 + num1, sum2 + num2, sumsq1 + (num1 * num1), sumsq2 + (num2 * num2), productSum + (num1 * num2))
        }
        case (acc, _) => acc
      }

      if (count == BigDecimal(0)) None
      else {
        val cov = (productSum - ((sum1 * sum2) / count)) / count
        val stdDev1 = sqrt(count * sumsq1 - sum1 * sum1) / count
        val stdDev2 = sqrt(count * sumsq2 - sum2 * sum2) / count

        if ((stdDev1 == 0) || (stdDev2 == 0)) {
          None
        } else Some(SDecimal(cov / (stdDev1 * stdDev2)))
      }
    }
  }

  object Covariance extends BIF2(StatsNamespace, "cov") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = { 
      case (SDecimal(num1), SDecimal(num2)) => SArray(Vector(SDecimal(num1), SDecimal(num2)))  
    }

    override val requiresReduction = true

    override def reduced(enum: Dataset[SValue]): Option[SValue] = {             
      val (count, sum1, sum2, productSum) = enum.reduce((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
        case ((count, sum1, sum2, productSum), SArray(Vector(SDecimal(num1), SDecimal(num2)))) => {
          (count + 1, sum1 + num1, sum2 + num2, productSum + (num1 * num2))
        }
        case (acc, _) => acc
      }

      if (count == BigDecimal(0)) None
      else Some(SDecimal((productSum - ((sum1 * sum2) / count)) / count))
    }
  }

  object LinearRegression extends BIF2(StatsNamespace, "linReg") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = { 
      case (SDecimal(num1), SDecimal(num2)) => SArray(Vector(SDecimal(num1), SDecimal(num2)))  
    }

    override val requiresReduction = true

    override def reduced(enum: Dataset[SValue]): Option[SValue] = {
      val (count, sum1, sum2, sumsq1, productSum) = enum.reduce((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
        case ((count, sum1, sum2, sumsq1, productSum), SArray(Vector(SDecimal(num1), SDecimal(num2)))) => {
          (count + 1, sum1 + num1, sum2 + num2, sumsq1 + (num1 * num1), productSum + (num1 * num2))
        }
        case (acc, _) => acc
      }

      if (count == BigDecimal(0)) None
      else {
        val cov = (productSum - ((sum1 * sum2) / count)) / count
        val vari = (sumsq1 - (sum1 * (sum1 / count))) / count

        val slope = cov / vari
        val yint = (sum2 / count) - (slope * (sum1 / count))
        
        Some(SObject(Map("slope" -> SDecimal(slope), "intercept" -> SDecimal(yint))))
      }
    }
  }

  object LogarithmicRegression extends BIF2(StatsNamespace, "logReg") {
    val operandType = (Some(SDecimal), Some(SDecimal))
    val operation: PartialFunction[(SValue, SValue), SValue] = { 
      case (SDecimal(num1), SDecimal(num2)) => SArray(Vector(SDecimal(num1), SDecimal(num2)))  
    }

    override val requiresReduction = true

    override def reduced(enum: Dataset[SValue]): Option[SValue] = {
      val (count, sum1, sum2, sumsq1, productSum) = enum.reduce((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
        case ((count, sum1, sum2, sumsq1, productSum), SArray(Vector(SDecimal(num1), SDecimal(num2)))) => {
          if (num1 > 0)
            (count + 1, sum1 + math.log(num1.toDouble), sum2 + num2, sumsq1 + (math.log(num1.toDouble) * math.log(num1.toDouble)), productSum + (math.log(num1.toDouble) * num2))
          else 
            (count, sum1, sum2, sumsq1, productSum)
        }
        case (acc, _) => acc
      }

      if (count == BigDecimal(0)) None
      else {
        val cov = (productSum - ((sum1 * sum2) / count)) / count
        val vari = (sumsq1 - (sum1 * (sum1 / count))) / count

        val slope = cov / vari
        val yint = (sum2 / count) - (slope * (sum1 / count))
        
        Some(SObject(Map("slope" -> SDecimal(slope), "intercept" -> SDecimal(yint))))
      }
    }
  }

  trait RankFunction {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = { 
      case s @ SDecimal(r) => s
    }
  }

  object DenseRank extends BIF1(StatsNamespace, "denseRank") with RankFunction {
    override def evalEnum(enum: Dataset[SValue], graph: DepGraph, ctx: Context): Option[Dataset[SValue]] = {
      var count = 0
      var previous: Option[SValue] = Option.empty[SValue]

      val enum2 = enum.sortByValue(graph.memoId, ctx.memoizationContext)
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
      Some(enum3.sortByIdentity(IdGen.nextInt, ctx.memoizationContext))
    }
  }

  object Rank extends BIF1(StatsNamespace, "rank") with RankFunction {
    override def evalEnum(enum: Dataset[SValue], graph: DepGraph, ctx: Context): Option[Dataset[SValue]] = {
      var countTotal = 0
      var countEach = 1
      var previous: Option[SValue] = Option.empty[SValue]

      val enum2 = enum.sortByValue(graph.memoId, ctx.memoizationContext)
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
      Some(enum3.sortByIdentity(IdGen.nextInt, ctx.memoizationContext))
    }
  }
}
