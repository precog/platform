package com.precog
package daze

import bytecode.Library
import bytecode.Arity._

import yggdrasil._
import yggdrasil.table._

import com.precog.util.IdGen

import scalaz.Monoid
import scalaz.std.anyVal._

import org.apache.commons.collections.primitives.ArrayIntList

trait StatsLib extends GenOpcode
    with ReductionLib
    with ImplLibrary
    with BigDecimalOperations
    with Evaluator {

  import trans._
  import Count._
  import Mean._
  
  val StatsNamespace = Vector("std", "stats")
  val EmptyNamespace = Vector()

  override def _libMorphism = super._libMorphism ++ Set(Median, Mode, Covariance, LinearCorrelation, LinearRegression, LogarithmicRegression) 
  
  object Median extends Morphism(EmptyNamespace, "median", One) {
    


    /* def reduced(enum: Dataset[SValue], graph: DepGraph, ctx: Context): Option[SValue] = {
      val enum2 = enum.sortByValue(graph.memoId, ctx.memoizationContext)

      val count = enum.reduce(BigDecimal(0)) {
        case (count, SDecimal(v)) => count + 1
        case (acc, _) => acc
      }
      
      if (count == BigDecimal(0)) None
      else {
        val (c, median) = if (count.toInt % 2 == 0) {
          val index = (count.toInt / 2, (count.toInt / 2) + 1)
        
          enum2.reduce((BigDecimal(0), Option.empty[BigDecimal])) {
            case ((count, _), SDecimal(v)) if (count + 1 < index._2) => (count + 1, Some(v))
            case ((count, prev), SDecimal(v)) if (count + 1 == index._2) => {
              (count + 1, 
                if (prev.isDefined) prev map { x => (x + v) / 2 } 
                else None)  
            }
            case (acc, _) => acc
          } 
        } else {
          val index = (count.toInt / 2) + 1
        
          enum2.reduce(BigDecimal(0), Option.empty[BigDecimal]) {
            case ((count, _), SDecimal(_)) if (count + 1 < index) => (count + 1, None)
            case ((count, _), SDecimal(v)) if (count + 1 == index) => (count + 1, Some(v))
            case (acc, _) => acc
          }
        }
        if (median.isDefined) median map { v => SDecimal(v) }
        else None
      }
    } */
   
    lazy val alignment = None
    
    def apply(table: Table): Table = {  //TODO write tests for the empty table case
      val compactedTable = table.compact(Leaf(Source))

      val sortKey = DerefObjectStatic(Leaf(Source), constants.Value)
      val sortedTable = compactedTable.sort(sortKey, SortAscending)

      val count = sortedTable.reduce(Count.reducer)
      
      if (count % 2 == 0) {
        val middleValues = sortedTable.take((count / 2) + 1).drop((count / 2) - 1)
        Mean(middleValues)
        //Mean.extract(middleValues.reduce(Mean.reducer))

      } else {
        sortedTable.take((count / 2) + 1).drop(count / 2)
      }
    }
  }
  
  object Mode extends Morphism(EmptyNamespace, "mode", One) {
    /* def reduced(enum: Dataset[SValue], graph: DepGraph, ctx: Context): Option[SValue] = {
      val enum2 = enum.sortByValue(graph.memoId, ctx.memoizationContext)

      val (_, _, modes, _) = enum2.reduce(Option.empty[SValue], BigDecimal(0), List.empty[SValue], BigDecimal(0)) {
        case ((None, count, modes, maxCount), sv) => ((Some(sv), count + 1, List(sv), maxCount + 1))
        case ((Some(currentRun), count, modes, maxCount), sv) => {
          if (currentRun == sv) {
            if (count >= maxCount)
              (Some(sv), count + 1, List(sv), maxCount + 1)
            else if (count + 1 == maxCount)
              (Some(sv), count + 1, modes :+ sv, maxCount)
            else
              (Some(sv), count + 1, modes, maxCount)
          } else {
            if (maxCount == 1)
              (Some(sv), 1, modes :+ sv, maxCount)
            else
              (Some(sv), 1, modes, maxCount)
          }
        }

        case(acc, _) => acc
      }
      
      Some(SArray(Vector(modes: _*))) 
    } */
    
    lazy val alignment = None
    type Result = List[BigDecimal]  //(currentRunValue, curentCount, listOfModes, maxCount)
    
    implicit def monoid = new Monoid[BigDecimal] {  
      def zero = BigDecimal(0)
      def append(left: BigDecimal, right: => BigDecimal) = left + right
    }

    def reducer: Reducer[Result] = new Reducer[Result] {
      def reduce(col: Column, range: Range): Result = {
        col match {
          case col: LongColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              List.empty[BigDecimal]
            } else {
              val foldedMapped: (Option[BigDecimal], BigDecimal, List[BigDecimal], BigDecimal) = mapped.foldLeft(Option.empty[BigDecimal], BigDecimal(0), List.empty[BigDecimal], BigDecimal(0)) {
                case ((None, count, modes, maxCount), sv) => ((Some(sv), count + 1, List(sv), maxCount + 1))
                case ((Some(currentRun), count, modes, maxCount), sv) => {
                  if (currentRun == sv) {
                    if (count >= maxCount)
                      (Some(sv), count + 1, List(sv), maxCount + 1)
                    else if (count + 1 == maxCount)
                      (Some(sv), count + 1, modes :+ BigDecimal(sv), maxCount)
                    else
                      (Some(sv), count + 1, modes, maxCount)
                  } else {
                    if (maxCount == 1)
                      (Some(sv), 1, modes :+ BigDecimal(sv), maxCount)
                    else
                      (Some(sv), 1, modes, maxCount)
                  }
                }
              }

              val (_, _, result, _) = foldedMapped
              result
            }
          //case col: DoubleColumn => 
          //  val mapped = range filter col.isDefinedAt map { x => col(x) }
          //  if (mapped.isEmpty) {
          //    None
          //  } else {
          //    val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0))) {
          //      case ((sum, count), value) => (sum + value: BigDecimal, count + 1: BigDecimal)
          //    }

          //    Some(foldedMapped)
          //  }
          //case col: NumColumn => 
          //  val mapped = range filter col.isDefinedAt map { x => col(x) }
          //  if (mapped.isEmpty) {
          //    None
          //  } else {
          //    val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0))) {
          //      case ((sum, count), value) => (sum + value: BigDecimal, count + 1: BigDecimal)
          //    }

          //    Some(foldedMapped)
          //  }
        }
      }
    }

    def extract(res: Result): Table =
      res map { case (sum, count) => ops.constDecimal(sum / count) } getOrElse ops.empty

    def apply(table: Table) = {
      val sortKey = DerefObjectStatic(Leaf(Source), constants.Value)
      val sortedTable = table.sort(sortKey, SortAscending)

      

    }
  }
 
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
    lazy val alignment = None
    
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
    lazy val alignment = None
    
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
