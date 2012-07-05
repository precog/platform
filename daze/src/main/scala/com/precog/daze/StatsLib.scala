package com.precog
package daze

import bytecode.Library
import bytecode.Arity._

import yggdrasil._
import yggdrasil.table._

import com.precog.util.IdGen

trait StatsLib extends GenOpcode
    with ImplLibrary
    with BigDecimalOperations
    with Evaluator {
  
  val StatsNamespace = Vector("std", "stats")

  override def _libMorphism = super._libMorphism ++ Set(Covariance, LinearCorrelation, LinearRegression, LogarithmicRegression)
 
  object LinearCorrelation extends Morphism(StatsNamespace, "corr", Two) {
    lazy val alignment = Some(MorphismAlignment.Match)
    
    def apply(table: Table) = table
    
    /* override def reduced(enum: Dataset[SValue]): Option[SValue] = {              
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
    } */
  }

  object Covariance extends Morphism(StatsNamespace, "cov", Two) {
    lazy val alignment = Some(MorphismAlignment.Match)
    
    def apply(table: Table) = table
    
    /* override def reduced(enum: Dataset[SValue]): Option[SValue] = {             
      val (count, sum1, sum2, productSum) = enum.reduce((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
        case ((count, sum1, sum2, productSum), SArray(Vector(SDecimal(num1), SDecimal(num2)))) => {
          (count + 1, sum1 + num1, sum2 + num2, productSum + (num1 * num2))
        }
        case (acc, _) => acc
      }

      if (count == BigDecimal(0)) None
      else Some(SDecimal((productSum - ((sum1 * sum2) / count)) / count))
    } */
  }

  object LinearRegression extends Morphism(StatsNamespace, "linReg", Two) {
    lazy val alignment = Some(MorphismAlignment.Match)
    
    def apply(table: Table) = table
    
    /* override def reduced(enum: Dataset[SValue]): Option[SValue] = {
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
    } */
  }

  object LogarithmicRegression extends Morphism(StatsNamespace, "logReg", Two) {
    lazy val alignment = Some(MorphismAlignment.Match)
    
    def apply(table: Table) = table
    
    /* override def reduced(enum: Dataset[SValue]): Option[SValue] = {
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
    } */
  }

  object DenseRank extends Morphism(StatsNamespace, "denseRank", One) {
    def apply(table: Table) = table
    
    /* override def evalEnum(enum: Dataset[SValue], graph: DepGraph, ctx: Context): Option[Dataset[SValue]] = {
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
    } */
  }

  object Rank extends Morphism(StatsNamespace, "rank", One) {
    def apply(table: Table) = table
    
    /* override def evalEnum(enum: Dataset[SValue], graph: DepGraph, ctx: Context): Option[Dataset[SValue]] = {
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
    } */
  }
}
