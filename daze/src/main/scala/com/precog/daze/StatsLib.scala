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
import bytecode.Library

import yggdrasil._
import yggdrasil.table._

import com.precog.util.IdGen
import com.precog.util._

import scala.collection.BitSet

import scalaz._
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.std.tuple._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._
import scalaz.syntax.std.boolean._

import org.apache.commons.collections.primitives.ArrayIntList

trait StatsLib[M[+_]] extends GenOpcode[M] with ReductionLib[M] with BigDecimalOperations with Evaluator[M] {
  import trans._
  import TableModule.paths
  
  val StatsNamespace = Vector("std", "stats")
  val EmptyNamespace = Vector()

  override def _libMorphism1 = super._libMorphism1 ++ Set(Median, Mode, Rank, DenseRank) 
  override def _libMorphism2 = super._libMorphism2 ++ Set(Covariance, LinearCorrelation, LinearRegression, LogarithmicRegression) 
  
  object Median extends Morphism1(EmptyNamespace, "median") {
    
    import Mean._
    
    val tpe = UnaryOperationType(JNumberT, JNumberT)

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
   
    def apply(table: Table) = {  //TODO write tests for the empty table case
      val compactedTable = table.compact(Leaf(Source))

      val sortKey = DerefObjectStatic(Leaf(Source), paths.Value)

      for {
        sortedTable <- compactedTable.sort(sortKey, SortAscending)
        count <- sortedTable.reduce(Count.reducer)
        median <- if (count % 2 == 0) {
                    val middleValues = sortedTable.take((count.toLong / 2) + 1).drop((count.toLong / 2) - 1)
                    Mean(middleValues)
                  } else {
                    M.point(sortedTable.take((count.toLong / 2) + 1).drop(count.toLong / 2))
                  }
      } yield median
    }
  }
  
  object Mode extends Morphism1(EmptyNamespace, "mode") {
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
    
    type Result = Set[BigDecimal]  //(currentRunValue, curentCount, listOfModes, maxCount)
    
    val tpe = UnaryOperationType(JNumberT, JNumberT)

    implicit def monoid = new Monoid[BigDecimal] {  
      def zero = BigDecimal(0)
      def append(left: BigDecimal, right: => BigDecimal) = left + right
    }    

    implicit def setMonoid[A] = new Monoid[Set[A]] {  //TODO this is WAY WRONG - it needs to deal with slice boundaries properly!!
      def zero = Set.empty[A]
      def append(left: Set[A], right: => Set[A]) = left ++ right
    }

    def reducer: Reducer[Result] = new Reducer[Result] {  //TODO add cases for other column types; get information necessary for dealing with slice boundaries and unsoretd slices in the Iterable[Slice] that's used in table.reduce
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        cols(JNumberT) flatMap {
          case col: LongColumn => 
            val mapped = range filter col.isDefinedAt map { x => col(x) }
            if (mapped.isEmpty) {
              Set.empty[BigDecimal]
            } else {
              val foldedMapped: (Option[BigDecimal], BigDecimal, Set[BigDecimal], BigDecimal) = mapped.foldLeft(Option.empty[BigDecimal], BigDecimal(0), Set.empty[BigDecimal], BigDecimal(0)) {
                case ((None, count, modes, maxCount), sv) => ((Some(sv), count + 1, Set(sv), maxCount + 1))
                case ((Some(currentRun), count, modes, maxCount), sv) => {
                  if (currentRun == sv) {
                    if (count >= maxCount)
                      (Some(sv), count + 1, Set(sv), maxCount + 1)
                    else if (count + 1 == maxCount)
                      (Some(sv), count + 1, modes + BigDecimal(sv), maxCount)
                    else
                      (Some(sv), count + 1, modes, maxCount)
                  } else {
                    if (maxCount == 1)
                      (Some(sv), 1, modes + BigDecimal(sv), maxCount)
                    else
                      (Some(sv), 1, modes, maxCount)
                  }
                }
              }

              val (_, _, result, _) = foldedMapped
              result
            }

          case _ => Set.empty[BigDecimal]
        }
      }
    }

    def extract(res: Result): Table = {
      val setC = res map CNum.apply
      ops.constDecimal(setC)
    }

    def apply(table: Table) = {
      val sortKey = DerefObjectStatic(Leaf(Source), paths.Value)
      val sortedTable: M[Table] = table.sort(sortKey, SortAscending)

      sortedTable.flatMap(_.reduce(reducer).map(extract))
    }
  }
 
  object LinearCorrelation extends Morphism2(StatsNamespace, "corr") {
    val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
    
    lazy val alignment = MorphismAlignment.Match

    type InitialResult = (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal) // (count, sum1, sum2, sumsq1, sumsq2, productSum)
    type Result = Option[(BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal)] // (count, sum1, sum2, sumsq1, sumsq2, productSum)

    implicit def monoid = implicitly[Monoid[Result]]
    
    def reducer: Reducer[Result] = new Reducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {

        val left = cols(JArrayFixedT(Map(0 -> JNumberT))) 
        val right = cols(JArrayFixedT(Map(1 -> JNumberT))) 

        val cross = for (l <- left; r <- right) yield (l, r)

        val result = cross flatMap {
          case (c1: LongColumn, c2: LongColumn) => 
            val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 + v2), productSum + (v1 * v2))
              }

              Some(foldedMapped)
            }

          case _ => None
        }

        (result.isEmpty).option(result.suml)
      }
    }
    
    def extract(res: Result): Table = {
      val res2 = res filter { 
        case (count, sum1, sum2, sumsq1, sumsq2, _) => {
          (count != 0) && (count * sumsq1 - sum1 * sum1 != 0) && (count * sumsq2 - sum2 * sum2 != 0) 
        }
      } 
      
      res2 map { 
        case (count, sum1, sum2, sumsq1, sumsq2, productSum) => {
          val cov = (productSum - ((sum1 * sum2) / count)) / count
          val stdDev1 = sqrt(count * sumsq1 - sum1 * sum1) / count
          val stdDev2 = sqrt(count * sumsq2 - sum2 * sum2) / count

          ops.constDecimal(Set(CNum(cov / (stdDev1 * stdDev2))))
        }
      } getOrElse ops.empty
    }

    def apply(table: Table) = table.reduce(reducer) map extract
    
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

  object Covariance extends Morphism2(StatsNamespace, "cov") {
    val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

    lazy val alignment = MorphismAlignment.Match

    type InitialResult = (BigDecimal, BigDecimal, BigDecimal, BigDecimal) // (count, sum1, sum2, productSum)
    type Result = Option[(BigDecimal, BigDecimal, BigDecimal, BigDecimal)] // (count, sum1, sum2, productSum)

    implicit def monoid = implicitly[Monoid[Result]]
    
    def reducer: Reducer[Result] = new Reducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {

        val left = cols(JArrayFixedT(Map(0 -> JNumberT))) 
        val right = cols(JArrayFixedT(Map(1 -> JNumberT))) 

        val cross = for (l <- left; r <- right) yield (l, r)

        val result = cross flatMap {
          case (c1: LongColumn, c2: LongColumn) => 
            val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
              }

              Some(foldedMapped)
            }

          case _ => None
        }

        (result.isEmpty).option(result.suml)
      }
    }
    
    def extract(res: Result): Table = {
      val res2 = res filter { 
        case (count, _, _, _) => count != 0
      } 
      
      res2 map { 
        case (count, sum1, sum2, productSum) => {
          val cov = (productSum - ((sum1 * sum2) / count)) / count

          ops.constDecimal(Set(CNum(cov)))
        }
      } getOrElse ops.empty
    }

    def apply(table: Table) = table.reduce(reducer) map extract

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

  object LinearRegression extends Morphism2(StatsNamespace, "linReg") {
    val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

    lazy val alignment = MorphismAlignment.Match

    type InitialResult = (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal) // (count, sum1, sum2, sumsq1, productSum)
    type Result = Option[(BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal)] // (count, sum1, sum2, sumsq1, productSum)

    implicit def monoid = implicitly[Monoid[Result]]
    
    def reducer: Reducer[Result] = new Reducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {

        val left = cols(JArrayFixedT(Map(0 -> JNumberT))) 
        val right = cols(JArrayFixedT(Map(1 -> JNumberT))) 

        val cross = for (l <- left; r <- right) yield (l, r)

        val result = cross flatMap {
          case (c1: LongColumn, c2: LongColumn) => 
            val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
              }

              Some(foldedMapped)
            }

          case _ => None
        }

        (result.isEmpty).option(result.suml)
      }
    }
    
    def extract(res: Result): Table = {
      val res2 = res filter { 
        case (count, _, _, _, _) => count != 0
      } 
      
      res2 map { 
        case (count, sum1, sum2, sumsq1, productSum) => {
          val cov = (productSum - ((sum1 * sum2) / count)) / count
          val vari = (sumsq1 - (sum1 * (sum1 / count))) / count

          val slope = cov / vari
          val yint = (sum2 / count) - (slope * (sum1 / count))


          ops.constDecimal(Set(CNum(slope)))  //TODO want to return an object with two fields, one for slope and one for yint
        }
      } getOrElse ops.empty
    }

    def apply(table: Table) = table.reduce(reducer) map extract

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

  object LogarithmicRegression extends Morphism2(StatsNamespace, "logReg") {
    val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

    lazy val alignment = MorphismAlignment.Match

    type InitialResult = (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal) // (count, sum1, sum2, sumsq1, productSum)
    type Result = Option[(BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal)] // (count, sum1, sum2, sumsq1, productSum)

    implicit def monoid = implicitly[Monoid[Result]]
    
    def reducer: Reducer[Result] = new Reducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {

        val left = cols(JArrayFixedT(Map(0 -> JNumberT))) 
        val right = cols(JArrayFixedT(Map(1 -> JNumberT))) 

        val cross = for (l <- left; r <- right) yield (l, r)

        val result = cross flatMap {
          case (c1: LongColumn, c2: LongColumn) => 
            val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
            if (mapped.isEmpty) {
              None
            } else {
              val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                  if (v1 > 0) {
                    (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                  } else {
                  (count, sum1, sum2, sumsq1, productSum)
                  }
                }
              }

              Some(foldedMapped)
            }

          case _ => None
        }

        (result.isEmpty).option(result.suml)
      }
    }
    
    def extract(res: Result): Table = {
      val res2 = res filter { 
        case (count, _, _, _, _) => count != 0
      } 
      
      res2 map { 
        case (count, sum1, sum2, sumsq1, productSum) => {
          val cov = (productSum - ((sum1 * sum2) / count)) / count
          val vari = (sumsq1 - (sum1 * (sum1 / count))) / count

          val slope = cov / vari
          val yint = (sum2 / count) - (slope * (sum1 / count))


          ops.constDecimal(Set(CNum(slope)))  //TODO want to return an object with two fields, one for slope and one for yint
        }
      } getOrElse ops.empty
    }

    def apply(table: Table) = table.reduce(reducer) map extract

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

  object DenseRank extends Morphism1(StatsNamespace, "denseRank") {
    val tpe = UnaryOperationType(JNumberT, JNumberT)

    def rankScanner: CScanner = {
      new CScanner {
        type A = (Option[BigDecimal], BigDecimal)  // (value, count)
        val init = (None, BigDecimal(0))

        def scan(a: A, col: Column, range: Range): (A, Option[Column]) = {
          col match {
            case lc: LongColumn => {
              val filteredRange = range filter lc.isDefinedAt
              val defined: BitSet = BitSet(filteredRange: _*)

              val ((finalValue, finalCount), acc) = filteredRange.foldLeft((a, new Array[BigDecimal](range.end))) {
                case (((value, count), acc), i) => {
                  if (value == None) {  //TODO best way to deal with the None case, which occurs only on the first fold
                    acc(i) = 1
                    ((Some(BigDecimal(lc(i))), 1), acc)
                  } else if (Some(BigDecimal(lc(i))) == value) {
                    acc(i) = count
                    ((Some(BigDecimal(lc(i))), count), acc)
                  } else  {
                    acc(i) = count + 1
                    ((Some(BigDecimal(lc(i))), count + 1), acc)
                  }
                }
              }

              ((finalValue, finalCount), Some(ArrayNumColumn(defined, acc)))
            }

          case _ => (a, None)
          }
        }
      }
    }
    
    def apply(table: Table) = {
      val sortKey = DerefObjectStatic(Leaf(Source), paths.Value)
      val sortedTable = table.sort(sortKey, SortAscending)

      val transScan = Scan(DerefObjectStatic(Leaf(Source), paths.Value), rankScanner)
      
      sortedTable.map(_.transform(transScan))
    }



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

  object Rank extends Morphism1(StatsNamespace, "rank") {  //TODO what happens across slices??
    val tpe = UnaryOperationType(JNumberT, JNumberT)
    
    def rankScanner: CScanner = {
      new CScanner {
        type A = (Option[BigDecimal], BigDecimal, BigDecimal)  // (value, countEach, countTotal)
        val init = (None, BigDecimal(0), BigDecimal(0))

        def scan(a: A, col: Column, range: Range): (A, Option[Column]) = {
          col match {
            case lc: LongColumn => {
              val filteredRange = range filter lc.isDefinedAt
              val defined: BitSet = BitSet(filteredRange: _*)

              val ((finalValue, finalCountEach, finalCountTotal), acc) = filteredRange.foldLeft((a, new Array[BigDecimal](range.end))) {
                case (((value, countEach, countTotal), acc), i) => {
                  if (value == None) {  //TODO best way to deal with the None case, which occurs only on the first fold
                    acc(i) = 1
                    ((Some(BigDecimal(lc(i))), 1, 1), acc)
                  } else if (Some(BigDecimal(lc(i))) == value) {
                    acc(i) = countTotal
                    ((Some(BigDecimal(lc(i))), countEach + 1, countTotal), acc)
                  } else  {
                    acc(i) = countEach + countTotal
                    ((Some(BigDecimal(lc(i))), 1, countEach + countTotal), acc)
                  }
                }
              }

              ((finalValue, finalCountEach, finalCountTotal), Some(ArrayNumColumn(defined, acc)))
            }

          case _ => (a, None)
          }
        }
      }
    }
    
    def apply(table: Table) = {
      val sortKey = DerefObjectStatic(Leaf(Source), paths.Value)
      val sortedTable = table.sort(sortKey, SortAscending)

      val transScan = Scan(DerefObjectStatic(Leaf(Source), paths.Value), rankScanner)
      
      sortedTable.map(_.transform(transScan))
    }


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
