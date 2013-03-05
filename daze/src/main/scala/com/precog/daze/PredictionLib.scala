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

import common.json._

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

  trait PredictionSupport extends ColumnarTableLib with ModelSupport {
    trait PredictionBase extends ModelBase {
      protected def morph1Apply(models: Models, function: Double => Double): Morph1Apply = new Morph1Apply {
        def scanner(modelSet: ModelSet): CScanner = new CScanner {
          type A = Unit
          def init: A = ()

          def scan(a: A, cols: Map[ColumnRef, Column], range: Range): (A, Map[ColumnRef, Column]) = {
            def included(model: Model): Map[ColumnRef, Column] = {
              val featurePaths = model.featureValues.keySet

              val res = cols filter { case (ColumnRef(cpath, ctype), col) =>
                featurePaths.contains(cpath)
              }

              val resPaths = res map { case (ColumnRef(cpath, _), _) => cpath } toSet

              if (resPaths == featurePaths) res
              else Map.empty[ColumnRef, Column]
            }

            def defined(cols: Map[ColumnRef, Column]): BitSet = {
              val columns = cols map { case (_, col) => col }

              BitSetUtil.filteredRange(range) { i =>
                if (columns.isEmpty) false
                else columns.forall(_ isDefinedAt i)
              }
            }

            def filteredRange(cols: Map[ColumnRef, Column]) = range.filter(defined(cols).apply)

            val result: Set[Map[ColumnRef, Column]] = {

              val modelsResult: Set[Map[ColumnRef, Column]] = modelSet.models map { case model =>
                val includedModel = included(model)
                val definedModel = defined(includedModel)

                val resultArray = filteredRange(includedModel).foldLeft(new Array[Double](range.end)) { case (arr, i) =>
                  val cpaths = includedModel.map { case (ColumnRef(cpath, _), _) => cpath }.toSeq sorted

                  val modelDoubles = cpaths map { model.featureValues(_) }

                  val includedCols = includedModel.collect { case (ColumnRef(cpath, _), col: DoubleColumn) => (cpath, col) }.toMap
                  val includedDoubles = cpaths map { includedCols(_).apply(i) }

                  assert(modelDoubles.length == includedDoubles.length)
                  val res = (modelDoubles.zip(includedDoubles)).map { case (d1, d2) => d1 * d2 }.sum + model.constant

                  arr(i) = function(res)
                  arr
                }

                Map(ColumnRef(CPath(TableModule.paths.Value, CPathField(model.name)), CDouble) -> ArrayDoubleColumn(definedModel, resultArray))
              } 
              
              val identitiesResult: Map[ColumnRef, Column] = {
                val featureCols = cols collect { 
                  case (ColumnRef(CPath(TableModule.paths.Key, CPathIndex(idx)), ctype), col) => 
                    val path = Seq(TableModule.paths.Key, CPathIndex(idx))
                    (ColumnRef(CPath(path: _*), ctype), col)
                  case c @ (ColumnRef(CPath(TableModule.paths.Key), _), _) => c
                }

                val shift = featureCols.size
                val modelIds = modelSet.identity collect { case id if id.isDefined => id.get } toArray

                val modelCols: Map[ColumnRef, Column] = modelIds.zipWithIndex map { case (id, idx) => 
                  (ColumnRef(CPath(TableModule.paths.Key, CPathIndex(idx + shift)), CLong), Column.const(id))
                } toMap

                modelCols ++ featureCols
              }

              modelsResult ++ Set(identitiesResult)
            }

            implicit val semigroup = Column.unionRightSemigroup
            val monoidCols = implicitly[Monoid[Map[ColumnRef, Column]]]

            val reduced: Map[ColumnRef, Column] = result.toSet.suml(monoidCols)

            ((), reduced)
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
