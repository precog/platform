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

import yggdrasil._
import table._

import Jama._
import Jama.Matrix._

import scalaz._
import Scalaz._
import scalaz.std.anyVal._
import scalaz.std.tuple._

trait ModelSummaryModule[M[+_]] extends ColumnarTableLibModule[M] with ModelLibModule[M] {
  trait ModelSummarySupport extends ColumnarTableLib with ModelSupport {
    trait ModelSummaryBase extends ModelBase {
      protected def morph1Apply(models: Models, function: Double => Double): Morph1Apply = new Morph1Apply {

        /**
         * `rss` is the Residual Sum of Squares
         * `product` is the matrix product X'X, a symmetric matrix consisting of sums
         * `count` is the number of rows seen so far
         */
        case class ResultInfo(rss: Long, product: Matrix, count: Long)

        type Result = ResultInfo

        implicit val monoid = new Monoid[Result] {
          def zero = ResultInfo(0, new Matrix(Array()), 0)

          def append(t1: Result, t2: => Result) = {
            def isEmpty(matrix: Matrix) = matrix.getArray == Array(Array.empty[Double])

            val matrixSum = {
              if (isEmpty(t1.product)) {
                t2.product
              } else if (isEmpty(t2.product)) {
                t1.product
              } else { 
                assert(
                  t1.product.getColumnDimension == t2.product.getColumnDimension &&
                  t1.product.getRowDimension == t2.product.getRowDimension) 

                t1.product plus t2.product
              }
            }

            ResultInfo(
              t1.rss + t2.rss,
              matrixSum,
              t1.count + t2.count)
          }
        }

        def reducer(modelSet: ModelSet)(ctx: EvaluationContext): Reducer[Result] = new CReducer[Result] {
          def reduce(schema: CSchema, range: Range) = {
            sys.error("todo")
          }
        }

        def extract(result: Result): Table = sys.error("todo")
        
        def apply(table: Table, ctx: EvaluationContext): M[Table] = {
          val reducers: List[CReducer[Result]] = models map { modelSet => reducer(modelSet)(ctx) }

          val tables: M[Seq[Table]] = reducers map { red => table.reduce(red).map(extract) } sequence
          
          tables.map(_.reduceOption { _ concat _ } getOrElse Table.empty)
        }
      }
    }
  }
}


