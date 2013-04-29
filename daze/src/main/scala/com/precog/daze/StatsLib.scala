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

import scala.annotation.tailrec

import bytecode._
import bytecode.Library

import yggdrasil._
import yggdrasil.table._

import com.precog.common._

import com.precog.util.IdGen
import com.precog.util._

import org.apache.commons.collections.primitives.ArrayIntList

import com.precog.util.{BitSet, BitSetUtil}

import blueeyes.json._

import scalaz._
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.std.tuple._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._
import scalaz.syntax.std.option._
import scalaz.syntax.std.boolean._

import TableModule._

trait StatsLibModule[M[+_]] extends ColumnarTableLibModule[M] with EvaluatorMethodsModule[M] with ReductionLibModule[M] {
  import library._
  import trans._
  import constants._

  trait StatsLib extends ColumnarTableLib with EvaluatorMethods with ReductionLib {
    import BigDecimalOperations._
    import TableModule.paths
    
    val StatsNamespace = Vector("std", "stats")
    val EmptyNamespace = Vector()

    override def _libMorphism1 = super._libMorphism1 ++ Set(Median, Mode, Rank, DenseRank, IndexedRank, Dummy)
    override def _libMorphism2 = super._libMorphism2 ++ Set(Covariance, LinearCorrelation, LinearRegression, LogarithmicRegression) 
    
    object Median extends Morphism1(EmptyNamespace, "median") {
      import Mean._
      
      val tpe = UnaryOperationType(JNumberT, JNumberT)

      def apply(table: Table, ctx: EvaluationContext) = {  //TODO write tests for the empty table case
        val compactedTable = table.compact(WrapObject(Typed(DerefObjectStatic(Leaf(Source), paths.Value), JNumberT), paths.Value.name))

        val sortKey = DerefObjectStatic(Leaf(Source), paths.Value)

        for {
          sortedTable <- compactedTable.sort(sortKey, SortAscending)
          count <- sortedTable.reduce(Count.reducer(ctx))
          median <- if (count % 2 == 0) {
            val middleValues = sortedTable.takeRange((count.toLong / 2) - 1, 2)
            val transformedTable = middleValues.transform(trans.DerefObjectStatic(Leaf(Source), paths.Value))  //todo make function for this
            Mean(transformedTable, ctx)
          } else {
            val middleValue = M.point(sortedTable.takeRange((count.toLong / 2), 1))
            middleValue map { _.transform(trans.DerefObjectStatic(Leaf(Source), paths.Value)) }
          }
        } yield {
          val keyTable = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))
          val valueTable = median.transform(trans.WrapObject(Leaf(Source), paths.Value.name))
          
          valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
        }
      }
    }
  
    object Mode extends Morphism1(EmptyNamespace, "mode") {
      
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

      def reducer(ctx: EvaluationContext): Reducer[Result] = new Reducer[Result] {  //TODO add cases for other column types; get information necessary for dealing with slice boundaries and unsoretd slices in the Iterable[Slice] that's used in table.reduce
        def reduce(schema: CSchema, range: Range): Result = {
          schema.columns(JNumberT) flatMap {
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

      def extract(res: Result): Table = Table.constDecimal(res)

      def apply(table: Table, ctx: EvaluationContext) = {
        val sortKey = DerefObjectStatic(Leaf(Source), paths.Value)
        val sortedTable: M[Table] = table.sort(sortKey, SortAscending)

        sortedTable.flatMap(_.reduce(reducer(ctx)).map(extract))
      }
    }
    
    object LinearCorrelation extends Morphism2(StatsNamespace, "corr") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)
      
      lazy val alignment = MorphismAlignment.Match(M.point(morph1))

      type InitialResult = (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal) // (count, sum1, sum2, sumsq1, sumsq2, productSum)
      type Result = Option[(BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal)]

      implicit def monoid = implicitly[Monoid[Result]]
      
      def reducer(ctx: EvaluationContext): Reducer[Result] = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {
          val left = schema.columns(JArrayFixedT(Map(0 -> JNumberT)))
          val right = schema.columns(JArrayFixedT(Map(1 -> JNumberT)))

          val cross = for (l <- left; r <- right) yield (l, r)

          val result: Set[Result] = cross map {
            case (c1: LongColumn, c2: LongColumn) =>
              val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: NumColumn) =>
              val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: DoubleColumn) =>
              val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: NumColumn) =>
              val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: DoubleColumn) =>
              val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: LongColumn) =>
              val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: DoubleColumn) =>
              val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: LongColumn) =>
              val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: NumColumn) =>
              val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, sumsq2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), sumsq2 + (v2 * v2), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case _ => None
          }

          if (result.isEmpty) None
          else result.suml(monoid)
        }
      }
      
      def extract(res: Result): Table = {
        res filter (_._1 > 0) map { case (count, sum1, sum2, sumsq1, sumsq2, productSum) =>
            val unscaledVar1 = count * sumsq1 - sum1 * sum1
            val unscaledVar2 = count * sumsq2 - sum2 * sum2
            if (unscaledVar1 != 0 && unscaledVar2 != 0) {
              val cov = (productSum - ((sum1 * sum2) / count)) / count
              val stdDev1 = sqrt(unscaledVar1) / count
              val stdDev2 = sqrt(unscaledVar2) / count
              val correlation = cov / (stdDev1 * stdDev2)

              val resultTable = Table.constDecimal(Set(correlation))  //TODO the following lines are used throughout. refactor!
              val valueTable = resultTable.transform(trans.WrapObject(Leaf(Source), paths.Value.name))
              val keyTable = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

              valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
            } else {
              Table.empty
            }
        } getOrElse Table.empty
      }

      private val morph1 = new Morph1Apply {
        def apply(table: Table, ctx: EvaluationContext) = {
          val valueSpec = DerefObjectStatic(TransSpec1.Id, paths.Value)
          table.transform(valueSpec).reduce(reducer(ctx)) map extract
        }
      }
    }

    object Covariance extends Morphism2(StatsNamespace, "cov") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

      lazy val alignment = MorphismAlignment.Match(M.point(morph1))

      type InitialResult = (BigDecimal, BigDecimal, BigDecimal, BigDecimal) // (count, sum1, sum2, productSum)
      type Result = Option[(BigDecimal, BigDecimal, BigDecimal, BigDecimal)]

      implicit def monoid = implicitly[Monoid[Result]]
      
      def reducer(ctx: EvaluationContext): Reducer[Result] = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {

          val left = schema.columns(JArrayFixedT(Map(0 -> JNumberT)))
          val right = schema.columns(JArrayFixedT(Map(1 -> JNumberT)))

          val cross = for (l <- left; r <- right) yield (l, r)

          val result = cross map {
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
            case (c1: NumColumn, c2: NumColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: DoubleColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: DoubleColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: NumColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: LongColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: DoubleColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: LongColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: NumColumn) =>
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

          if (result.isEmpty) None
          else result.suml(monoid)
        }
      }
      
      def extract(res: Result): Table = {
        val res2 = res filter {
          case (count, _, _, _) => count != 0
        }
        
        res2 map {
          case (count, sum1, sum2, productSum) => {
            val cov = (productSum - ((sum1 * sum2) / count)) / count

            val resultTable = Table.constDecimal(Set(cov))
            val valueTable = resultTable.transform(trans.WrapObject(Leaf(Source), paths.Value.name))
            val keyTable = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

            valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
          }
        } getOrElse Table.empty
      }

      private val morph1 = new Morph1Apply {
        def apply(table: Table, ctx: EvaluationContext) = {
          val valueSpec = DerefObjectStatic(TransSpec1.Id, paths.Value)
          table.transform(valueSpec).reduce(reducer(ctx)) map extract
        }
      }
    }

    object LinearRegression extends Morphism2(StatsNamespace, "linReg") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

      lazy val alignment = MorphismAlignment.Match(M.point(morph1))

      type InitialResult = (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal) // (count, sum1, sum2, sumsq1, productSum)
      type Result = Option[(BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal)]

      implicit def monoid = implicitly[Monoid[Result]]
      
      def reducer(ctx: EvaluationContext): Reducer[Result] = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {

          val left = schema.columns(JArrayFixedT(Map(0 -> JNumberT)))
          val right = schema.columns(JArrayFixedT(Map(1 -> JNumberT)))

          val cross = for (l <- left; r <- right) yield (l, r)

          val result = cross map {
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
            case (c1: NumColumn, c2: NumColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: DoubleColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: DoubleColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: NumColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: LongColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: NumColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: LongColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => (count + 1, sum1 + v1, sum2 + v2, sumsq1 + (v1 * v1), productSum + (v1 * v2))
                }

                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: DoubleColumn) =>
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
          
          if (result.isEmpty) None
          else result.suml(monoid)
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

            val constSlope = Table.constDecimal(Set(slope))
            val constIntercept = Table.constDecimal(Set(yint))

            val slopeSpec = trans.WrapObject(Leaf(SourceLeft), "slope")
            val yintSpec = trans.WrapObject(Leaf(SourceRight), "intercept")
            val concatSpec = trans.InnerObjectConcat(slopeSpec, yintSpec)

            val valueTable = constSlope.cross(constIntercept)(trans.WrapObject(concatSpec, paths.Value.name))
            val keyTable = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

            valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
          }
        } getOrElse Table.empty
      }

      private val morph1 = new Morph1Apply {
        def apply(table: Table, ctx: EvaluationContext) = {
          val valueSpec = DerefObjectStatic(TransSpec1.Id, paths.Value)
          table.transform(valueSpec).reduce(reducer(ctx)) map extract
        }
      }
    }

    object LogarithmicRegression extends Morphism2(StatsNamespace, "logReg") {
      val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

      lazy val alignment = MorphismAlignment.Match(M.point(morph1))

      type InitialResult = (BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal) // (count, sum1, sum2, sumsq1, productSum)
      type Result = Option[(BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal)]

      implicit def monoid = implicitly[Monoid[Result]]
      
      def reducer(ctx: EvaluationContext): Reducer[Result] = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {

          val left = schema.columns(JArrayFixedT(Map(0 -> JNumberT)))
          val right = schema.columns(JArrayFixedT(Map(1 -> JNumberT)))

          val cross = for (l <- left; r <- right) yield (l, r)

          val result = cross map {
            case (c1: LongColumn, c2: LongColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1, sum1 + math.log(v1.toDouble), sum2 + v2, sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)), productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: NumColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1, sum1 + math.log(v1.toDouble), sum2 + v2, sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)), productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: DoubleColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1, sum1 + math.log(v1.toDouble), sum2 + v2, sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)), productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: DoubleColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1, sum1 + math.log(v1.toDouble), sum2 + v2, sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)), productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: LongColumn, c2: NumColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1, sum1 + math.log(v1.toDouble), sum2 + v2, sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)), productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: LongColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1, sum1 + math.log(v1.toDouble), sum2 + v2, sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)), productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: DoubleColumn, c2: NumColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1, sum1 + math.log(v1.toDouble), sum2 + v2, sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)), productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: LongColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1, sum1 + math.log(v1.toDouble), sum2 + v2, sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)), productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }
            case (c1: NumColumn, c2: DoubleColumn) =>
              val mapped = range filter ( r => c1.isDefinedAt(r) && c2.isDefinedAt(r)) map { i => (c1(i), c2(i)) }
              if (mapped.isEmpty) {
                None
              } else {
                val foldedMapped: InitialResult = mapped.foldLeft((BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0), BigDecimal(0))) {
                  case ((count, sum1, sum2, sumsq1, productSum), (v1, v2)) => {
                    if (v1 > 0) {
                      (count + 1, sum1 + math.log(v1.toDouble), sum2 + v2, sumsq1 + (math.log(v1.toDouble) * math.log(v1.toDouble)), productSum + (math.log(v1.toDouble) * v2))
                    } else {
                      (count, sum1, sum2, sumsq1, productSum)
                    }
                  }
                }
                Some(foldedMapped)
              }

            case _ => None
          }

          if (result.isEmpty) None
          else result.suml(monoid)
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

            val constSlope = Table.constDecimal(Set(slope))
            val constIntercept = Table.constDecimal(Set(yint))

            val slopeSpec = trans.WrapObject(Leaf(SourceLeft), "slope")
            val yintSpec = trans.WrapObject(Leaf(SourceRight), "intercept")
            val concatSpec = trans.InnerObjectConcat(slopeSpec, yintSpec)

            val valueTable = constSlope.cross(constIntercept)(trans.WrapObject(concatSpec, paths.Value.name))
            val keyTable = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

            valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
          }
        } getOrElse Table.empty
      }

      private val morph1 = new Morph1Apply {
        def apply(table: Table, ctx: EvaluationContext) = {
          val valueSpec = DerefObjectStatic(TransSpec1.Id, paths.Value)
          table.transform(valueSpec).reduce(reducer(ctx)) map extract
        }
      }
    }

    /**
     * Base trait for all Rank-scanners.
     *
     * This provides scaffolding that is useful in all cases.
     */
    trait BaseRankScanner extends CScanner[M] {
      import scala.collection.mutable

      // collapses number columns into one decimal column.
      //
      // TODO: it would be nice to avoid doing this, but its not clear that
      // decimals are going to be a significant performance problem for rank
      // (compared to sorting) and this simplifies the algorithm a lot.
      protected def decimalize(m: Map[ColumnRef, Column], r: Range): Map[ColumnRef, Column] = {
        val m2 = mutable.Map.empty[ColumnRef, Column]
        val nums = mutable.Map.empty[CPath, List[Column]]

        m.foreach { case (ref @ ColumnRef(path, ctype), col) =>
          if (ctype == CLong || ctype == CDouble || ctype == CNum) {
            nums(path) = col :: nums.getOrElse(path, Nil)
          } else {
            m2(ref) = col
          }
        }

        val start = r.start
        val end = r.end
        val len = r.size

        nums.foreach {
          case (path, cols) =>
            val bs = new BitSet()
            val arr = new Array[BigDecimal](len)
            cols.foreach {
              case col: LongColumn =>
                val bs2 = col.definedAt(start, end)
                Loop.range(0, len)(j => if (bs2.get(j)) arr(j) = BigDecimal(col(j + start)))
                bs.or(bs2)
              case col: DoubleColumn =>
                val bs2 = col.definedAt(start, end)
                Loop.range(0, len)(j => if (bs2.get(j)) arr(j) = BigDecimal(col(j + start)))
                bs.or(bs2)
              case col: NumColumn =>
                val bs2 = col.definedAt(start, end)
                Loop.range(0, r.size)(j => if (bs2.get(j)) arr(j) = col(j + start))
                bs.or(bs2)
              case col =>
                sys.error("unexpected column found: %s" format col)
            }

            m2(ColumnRef(path, CNum)) = shiftColumn(ArrayNumColumn(bs, arr), start)
        }
        m2.toMap
      }

      /**
       * Represents the state of the scanner at the end of a slice.
       *
       * The n is the last rank number used (-1 means we haven't started).
       * The iterable items are the refs/cvalues defined by that row (which
       * will only be empty when we haven't started).
       */
      case class RankContext(curr: Long, next: Long, items: Iterable[(ColumnRef, CValue)])

      type A = RankContext

      def init = RankContext(-1L, 0L, Nil)

      /**
       * Builds a bitset for each column we were given. Each bitset contains
       * (end-start) boolean values.
       */
      def initDefinedCols(cols: Array[Column], r: Range): Array[BitSet] = {
        val start = r.start
        val end = r.end
        val ncols = cols.length
        val arr = new Array[BitSet](ncols)
        var i = 0
        while (i < ncols) { arr(i) = cols(i).definedAt(start, end); i += 1 }
        arr
      }

      /**
       * Builds a bitset with a boolean value for each row. This value for a row
       * will be true if at least one column is defined for that row and false
       * otherwise.
       */
      def initDefined(definedCols: Array[BitSet]): BitSet = {
        val ncols = definedCols.length
        if (ncols == 0) return new BitSet()
        val arr = definedCols(0).copy()
        var i = 1
        while (i < ncols) { arr.or(definedCols(i)); i += 1 }
        arr
      }

      def findFirstDefined(defined: BitSet, r: Range): Int = {
        var row = r.start
        val end = r.end
        while (row < end && !defined.get(row)) row += 1
        row
      }

      def buildRankContext(m: Map[ColumnRef, Column], lastRow: Int, curr: Long, next: Long): RankContext = {
        val items = m.filter {
          case (k, v) => v.isDefinedAt(lastRow)
        }.map {
          case (k, v) => (k, v.cValue(lastRow))
        }
        RankContext(curr, next, items)
      }

      // TODO: seems like shifting shouldn't need to return an Option.
      def shiftColumn(col: Column, start: Int): Column =
        if (start == 0) col else (col |> cf.util.Shift(start)).get
    }

    /**
     * This class works for the indexed rank case (where we are not worried
     * about row uniqueness). It is much faster than the traits based on
     * UniqueRankScanner.
     */
    class IndexedRankScanner extends BaseRankScanner {
      def buildRankArrayIndexed(defined: BitSet, r: Range, ctxt: RankContext): (Array[Long], Long, Int) = {
        var curr = ctxt.next

        val start = r.start
        val end = r.end
        val len = end - start
        var i = 0
        val values = new Array[Long](len)

        var lastRow = -1
        while (i < len) {
          if (defined.get(i)) {
            lastRow = i
            values(i) = curr
            curr += 1L
          }
          i += 1
        }

        (values, curr, lastRow)
      }

      /**
       *
       */
      def scan(ctxt: RankContext, _m: Map[ColumnRef, Column], range: Range): M[(RankContext, Map[ColumnRef, Column])] = {

        val m = decimalize(_m, range)

        val start = range.start
        val end = range.end
        val len = end - start

        val cols = m.values.toArray

        // for each column, store its definedness bitset for later use
        val definedCols = initDefinedCols(cols, range)

        // find the union of column-definedness. any row in defined that is zero
        // after this is a row that is totally undefined.
        val defined = initDefined(definedCols)

        // find the first defined row
        val row = findFirstDefined(defined, range)

        // if none of our rows are defined let's short-circuit out of here!
        val back = if (row == end) { 
          (ctxt, Map.empty[ColumnRef, Column])
        } else {
          // build the actual rank array
          val (values, curr, lastRow) = buildRankArrayIndexed(defined, range, ctxt)
  
          // build the context to be used for the next slice
          val ctxt2 = buildRankContext(m, lastRow, curr, curr + 1L)
  
          // construct the column ref and column to return
          val col2: Column = shiftColumn(ArrayLongColumn(defined, values), start)
          val data = Map(ColumnRef(CPath.Identity, CLong) -> col2)
  
          (ctxt2, data)
        }
        
        M point back
      }
    }

    /**
     * This trait works for cases where the rank should be shared for adjacent
     * rows that are identical.
     */
    trait UniqueRankScanner extends BaseRankScanner {
      import scala.collection.mutable

      /**
       * Determines whether row is a duplicate of lastRow.
       * The method returns true if the two rows differ, e.g.:
       *
       * 1. There is a column that is defined for one row but not another.
       * 2. There is a column that is defined for both but has different values.
       *
       * If we make it through all the columns without either of those being
       * true then we can return false, since the rows are not duplicates.
       */
      @tailrec
      private def isDuplicate(cols: Array[Column], definedCols: Array[BitSet], lastRow: Int, row: Int, index: Int): Boolean = {
        if (index >= cols.length) {
          true
        } else {
          val dc = definedCols(index)
          val wasDefined = dc.get(lastRow)
          if (wasDefined != dc.get(row)) {
            false
          } else if (!wasDefined || cols(index).rowEq(lastRow, row)) {
            isDuplicate(cols, definedCols, lastRow, row, index + 1)
          } else {
            false
          }
        }
      }

      /**
       * Determines whether row is a duplicate of the row in RankContext.
       *
       * The basic idea is the same as isDuplicate() but in this case we're
       * comparing this row to a row from another slice. The easiest way to
       * do this is to just create a map of the old slice's column refs and
       * values. Then we can remove the refs/values for the current row and
       * see if any are missing from one or the other.
       *
       * We remove ref/cvalue pairs as we find them in the current row. If
       * we are missing a key, or find a key whose values differ we return
       * false. At the end, if the map is empty, we can return true (since
       * all the map's values were accounted for by this row). Otherwise we
       * return false.
       */
      def isDuplicateFromContext(ctxt: RankContext, refs: Array[ColumnRef], cols: Array[Column], row: Int): Boolean = {
        val m = mutable.Map.empty[ColumnRef, CValue]
        ctxt.items.foreach { case (ref, cvalue) => m(ref) = cvalue }
        var i = 0
        while (i < cols.length) {
          val col = cols(i)
          if (col.isDefinedAt(row)) {
            val opt = m.remove(refs(i))
            if (!opt.isDefined || opt.get != col.cValue(row)) return false
          }
          i += 1
        }
        m.isEmpty
      }

      def findDuplicates(defined: BitSet, definedCols: Array[BitSet], cols: Array[Column], r: Range, _row: Int): (BitSet, Int) = {
        val start = r.start
        val end = r.end
        val len = end - start
        var row = _row

        // start assuming all rows are dupes until we find out otherwise
        val duplicateRows = new BitSet()
        duplicateRows.setBits(Array.fill(((len - 1) >> 6) + 1)(-1L))

        // FIXME: for now, assume first row is valid
        duplicateRows.clear(row - start)
        var lastRow = row
        row += 1

        // compare each subsequent row against the last valid row
        while (row < end) {
          if (defined.get(row) && !isDuplicate(cols, definedCols, lastRow, row, 0)) {
            duplicateRows.clear(row - start)
            lastRow = row
          }
          row += 1
        }

        (duplicateRows, lastRow)
      }

      def buildRankArrayUnique(defined: BitSet, duplicateRows: BitSet, r: Range, ctxt: RankContext): (Array[Long], Long, Long)

      /**
       *
       */
      def scan(ctxt: RankContext, _m: Map[ColumnRef, Column], range: Range): M[(RankContext, Map[ColumnRef, Column])] = {
        val m = decimalize(_m, range)

        val start = range.start
        val end = range.end
        val len = end - start

        val cols = m.values.toArray

        // for each column, store its definedness bitset for later use
        val definedCols = initDefinedCols(cols, range)

        // find the union of column-definedness. any row in defined that is zero
        // after this is a row that is totally undefined.
        val defined = initDefined(definedCols)

        // find the first defined row
        val row = findFirstDefined(defined, range)

        // if none of our rows are defined let's short-circuit out of here!
        val back = if (row == end) {
          (ctxt, Map.empty[ColumnRef, Column])
        } else {
          // find a bitset of duplicate rows and the last defined row
          val (duplicateRows, lastRow) = findDuplicates(defined, definedCols, cols, range, row)
  
          // build the actual rank array
          val (values, curr, next) = buildRankArrayUnique(defined, duplicateRows, range, ctxt)
  
          // build the context to be used for the next slice
          val ctxt2 = buildRankContext(m, lastRow, curr, next)
  
          // construct the column ref and column to return
          val col2 = shiftColumn(ArrayLongColumn(defined, values), start)
          val data = Map(ColumnRef(CPath.Identity, CLong) -> col2)
  
          (ctxt2, data)
        }
        
        M point back
      }
    }

    class SparseRankScaner extends UniqueRankScanner {
      def buildRankArrayUnique(defined: BitSet, duplicateRows: BitSet, r: Range, ctxt: RankContext): (Array[Long], Long, Long) = {
        var curr = ctxt.curr
        var next = ctxt.next

        val start = r.start
        val end = r.end
        val len = end - start
        var i = 0
        val values = new Array[Long](len)

        while (i < len) {
          if (defined.get(i)) {
            if (!duplicateRows.get(i)) curr = next
            values(i) = curr
            next += 1L
          }
          i += 1
        }

        (values, curr, next)
      }
    }

    class DenseRankScaner extends UniqueRankScanner {
      def buildRankArrayUnique(defined: BitSet, duplicateRows: BitSet, r: Range, ctxt: RankContext): (Array[Long], Long, Long) = {
        var curr = ctxt.curr

        val start = r.start
        val end = r.end
        val len = end - start
        var i = 0
        val values = new Array[Long](len)

        while (i < len) {
          if (defined.get(i)) {
            if (!duplicateRows.get(i)) curr += 1L
            values(i) = curr
          }
          i += 1
        }

        (values, curr, curr + 1L)
      }
    }

    final class DummyScanner(length: Long) extends CScanner[M] {
      type A = Long
      def init: A = 0L

      def scan(a: A, cols: Map[ColumnRef, Column], range: Range): M[(A, Map[ColumnRef, Column])] = {
        val defined = cols.values.foldLeft(BitSetUtil.create()) { (bitset, col) =>
          bitset.or(col.definedAt(range.start, range.end))
          bitset
        }

        val indices = new Array[Long](range.size)
        var i = 0
        var n: Long = a
        while (i < indices.length) {
          if (defined(i)) {
            indices(i) = n
            n += 1
          } else {
            indices(i) = -1
          }
          i += 1
        }

        def makeColumn(idx: Int) = new LongColumn {
          def isDefinedAt(row: Int) = defined(row)
          def apply(row: Int) = if (indices(row) == idx) 1L else 0L
        }

        val cols0: Map[ColumnRef, Column] = (0 until length.toInt).map({ idx =>
          ColumnRef(CPath(CPathIndex(idx)), CLong) -> makeColumn(idx)
        })(collection.breakOut)

        M point (n, cols0)
      }
    }

    abstract class BaseRank(name: String, retType: JType = JNumberT) extends Morphism1(StatsNamespace, name) {
      val tpe = UnaryOperationType(JType.JUniverseT, retType)
      override val idPolicy = IdentityPolicy.Retain.Merge
      
      def rankScanner(size: Long): CScanner[M]
      
      private val sortByValue = DerefObjectStatic(Leaf(Source), paths.Value)
      private val sortByKey = DerefObjectStatic(Leaf(Source), paths.Key)

      // TODO: sorting the data twice is kind of awful.
      //
      // it would be better to let our consumers worry about whether the table is
      // sorted on paths.SortKey.

      def apply(table: Table, ctx: EvaluationContext): M[Table] = {
        def count(tbl: Table): M[Long] = tbl.size match {
          case ExactSize(n) => M.point(n)
          case _ => table.reduce(Count.reducer(ctx))
        }

        for {
          valueSortedTable <- table.sort(sortByValue, SortAscending)
          size <- count(valueSortedTable)
          rankedTable = {
            val m = Map(paths.Value -> Scan(Leaf(Source), rankScanner(size)))
            val scan = makeTableTrans(m)
            valueSortedTable.transform(ObjectDelete(scan, Set(paths.SortKey)))
          }
          keySortedTable <- rankedTable.sort(sortByKey, SortAscending)
        } yield keySortedTable
      }
    }

    object DenseRank extends BaseRank("denseRank") {
      def rankScanner(size: Long) = new DenseRankScaner
    }

    object Rank extends BaseRank("rank") {
      def rankScanner(size: Long) = new SparseRankScaner
    }

    object IndexedRank extends BaseRank("indexedRank") {
      def rankScanner(size: Long) = new IndexedRankScanner
    }

    object Dummy extends BaseRank("dummy", JArrayUnfixedT) {
      def rankScanner(size: Long) = new DummyScanner(size)
    }
  }
}
