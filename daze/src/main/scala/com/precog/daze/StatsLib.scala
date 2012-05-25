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

import bytecode.Library
import bytecode.BuiltInFunc1
import bytecode.BuiltInFunc2

import yggdrasil._

trait StatsLib extends GenOpcode with ImplLibrary with DatasetOpsComponent with BigDecimalOperations {
  import ops.extend
  //private implicit def extend[E](d: Dataset[E]): DatasetExtensions[Dataset, Memoable, Grouping, E] = ops.extend(d)

  val StatsNamespace = Vector("std", "stats")

  override def _lib1 = super._lib1 ++ Set(DenseRank, Rank)
  override def _lib2 = super._lib2 ++ Set(Covariance, LinearCorrelation, LinearRegression, LogarithmicRegression)

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
        
        Some(SArray(Vector(SDecimal(slope), SDecimal(yint))))
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
        
        Some(SArray(Vector(SDecimal(slope), SDecimal(yint))))
      }
    }
  }

  trait RankFunction {
    val operandType = Some(SDecimal)
    val operation: PartialFunction[SValue, SValue] = { 
      case s @ SDecimal(r) => s
    }
  }

  object DenseRank extends BIF1(StatsNamespace, "denseRank") with RankFunction
  object Rank extends BIF1(StatsNamespace, "rank") with RankFunction

}
