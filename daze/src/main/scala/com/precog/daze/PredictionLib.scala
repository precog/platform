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

import com.precog.common._
import util._

import yggdrasil._
import table._

import bytecode._



import Jama._
import Jama.Matrix._

import org.apache.commons._
import math3.distribution._

import scalaz._
import Scalaz._
import scalaz.std.anyVal._
import scalaz.std.set._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._
import scalaz.syntax.std.boolean._
import scalaz.syntax.traverse._

trait PredictionLibModule[M[+_]] extends ColumnarTableLibModule[M] with ModelLibModule[M] {
  import trans._
  import trans.constants._

  trait PredictionSupport extends ColumnarTableLib with ModelSupport with RegressionSupport {
    val confIntvStr = "confidenceInterval"
    val predIntvStr = "predictionInterval"
    val fitStr = "fit"

    trait LinearPredictionBase extends LinearModelBase {
      protected def morph1Apply(models: Models, trans: Double => Double): Morph1Apply = new Morph1Apply {
        def scanner(modelSet: ModelSet): CScanner[M] = new CScanner[M] {
          type A = Unit
          def init: A = ()

          def scan(a: A, cols: Map[ColumnRef, Column], range: Range): M[(A, Map[ColumnRef, Column])] = {
            val result: Set[Map[ColumnRef, Column]] = {
              val modelsResult: Set[Map[ColumnRef, Column]] = modelSet.models map { case model =>
                val scannerPrelims = ModelLike.makePrelims(model, cols, range, trans)
                val resultArray = scannerPrelims.resultArray
                val definedModel = scannerPrelims.definedModel

                val dist = new TDistribution(model.degOfFreedom)
                val prob = 0.975
                val tStat = dist.inverseCumulativeProbability(prob)

                val varCovarMatrix = new Matrix(model.varCovar)

                case class Intervals(confidence: Array[Double], prediction: Array[Double])

                val res = ModelLike.filteredRange(scannerPrelims.includedModel, range).foldLeft(Intervals(new Array[Double](range.end), new Array[Double](range.end))) { case (Intervals(arrConf, arrPred), i) =>
                  val includedDoubles = 1.0 +: (scannerPrelims.cpaths map { scannerPrelims.includedCols(_).apply(i) })
                  val includedMatrix = new Matrix(Array(includedDoubles.toArray))

                  val prod = includedMatrix.times(varCovarMatrix).times(includedMatrix.transpose()).getArray

                  val inner = {
                    if (prod.length == 1 && prod.head.length == 1) prod(0)(0)
                    else sys.error("matrix of wrong shape")
                  }

                  val conf = math.sqrt(inner)
                  val pred = math.sqrt(math.pow(model.resStdErr, 2.0) + inner)

                  arrConf(i) = tStat * conf
                  arrPred(i) = tStat * pred

                  Intervals(arrConf, arrPred)
                }

                val confidenceUpper = arraySum(resultArray, res.confidence)
                val confidenceLower = arraySum(resultArray, res.confidence map { -_ })

                val predictionUpper = arraySum(resultArray, res.prediction)
                val predictionLower = arraySum(resultArray, res.prediction map { -_ })

                def makeCPath(field: String, index: Int) = {
                  CPath(TableModule.paths.Value, CPathField(model.name), CPathField(field), CPathIndex(index))
                }

                // the correct model name gets added to the CPath here
                val pathFit = CPath(TableModule.paths.Value, CPathField(model.name), CPathField(fitStr))
                val pathConfidenceLower = makeCPath(confIntvStr, 0)
                val pathConfidenceUpper = makeCPath(confIntvStr, 1)
                val pathPredictionLower = makeCPath(predIntvStr, 0)
                val pathPredictionUpper = makeCPath(predIntvStr, 1)

                Map(
                  ColumnRef(pathFit, CDouble) -> ArrayDoubleColumn(definedModel, resultArray),
                  ColumnRef(pathConfidenceUpper, CDouble) -> ArrayDoubleColumn(definedModel, confidenceUpper),
                  ColumnRef(pathConfidenceLower, CDouble) -> ArrayDoubleColumn(definedModel, confidenceLower),
                  ColumnRef(pathPredictionUpper, CDouble) -> ArrayDoubleColumn(definedModel, predictionUpper),
                  ColumnRef(pathPredictionLower, CDouble) -> ArrayDoubleColumn(definedModel, predictionLower))
              }

              val identitiesResult = ModelLike.idRes(cols, modelSet)

              modelsResult ++ Set(identitiesResult)
            }

            implicit val semigroup = Column.unionRightSemigroup
            val monoidCols = implicitly[Monoid[Map[ColumnRef, Column]]]
            val reduced: Map[ColumnRef, Column] = result.toSet.suml(monoidCols)

            M point ((), reduced)
          }
        }

        def apply(table: Table, ctx: EvaluationContext): M[Table] = {
          val scanners: Seq[TransSpec1] = models map { model => WrapArray(Scan(TransSpec1.Id, scanner(model))) }
          val spec: TransSpec1 = scanners reduceOption { (s1, s2) => InnerArrayConcat(s1, s2) } getOrElse TransSpec1.Id

          val forcedTable = table.transform(spec).force
          val tables0 = Range(0, scanners.size) map { i => forcedTable.map(_.transform(DerefArrayStatic(TransSpec1.Id, CPathIndex(i)))) }
          val tables: M[Seq[Table]] = (tables0.toList).sequence

          tables.map(_.reduceOption { _ concat _ } getOrElse Table.empty)
        }
      }
    }

    trait LogisticPredictionBase extends LogisticModelBase {
      protected def morph1Apply(models: Models, trans: Double => Double): Morph1Apply = new Morph1Apply {
        def scanner(modelSet: ModelSet): CScanner[M] = new CScanner[M] {
          type A = Unit
          def init: A = ()

          def scan(a: A, cols: Map[ColumnRef, Column], range: Range): M[(A, Map[ColumnRef, Column])] = {
            val result: Set[Map[ColumnRef, Column]] = {
              val modelsResult: Set[Map[ColumnRef, Column]] = modelSet.models map { case model =>
                val scannerPrelims = ModelLike.makePrelims(model, cols, range, trans)

                // the correct model name gets added to the CPath here
                val pathFit = CPath(TableModule.paths.Value, CPathField(model.name), CPathField(fitStr))

                Map(ColumnRef(pathFit, CDouble) -> ArrayDoubleColumn(scannerPrelims.definedModel, scannerPrelims.resultArray))
              }
              
              val identitiesResult = ModelLike.idRes(cols, modelSet)

              modelsResult ++ Set(identitiesResult)
            }

            implicit val semigroup = Column.unionRightSemigroup
            val monoidCols = implicitly[Monoid[Map[ColumnRef, Column]]]
            val reduced: Map[ColumnRef, Column] = result.toSet.suml(monoidCols)

            M point ((), reduced)
          }
        }

        def apply(table: Table, ctx: EvaluationContext): M[Table] = {
          val scanners: Seq[TransSpec1] = models map { model => WrapArray(Scan(TransSpec1.Id, scanner(model))) }
          val spec: TransSpec1 = scanners reduceOption { (s1, s2) => InnerArrayConcat(s1, s2) } getOrElse TransSpec1.Id

          val forcedTable = table.transform(spec).force
          val tables0 = Range(0, scanners.size) map { i => forcedTable.map(_.transform(DerefArrayStatic(TransSpec1.Id, CPathIndex(i)))) }
          val tables: M[Seq[Table]] = (tables0.toList).sequence

          tables.map(_.reduceOption { _ concat _ } getOrElse Table.empty)
        }
      }
    }
  }
}
