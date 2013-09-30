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
package mimir

import bytecode._

import yggdrasil._
import yggdrasil.table._
import TransSpecModule._

import com.precog.util.{BitSet, BitSetUtil}
import com.precog.util.BitSetUtil.Implicits._


import scalaz._
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.syntax.monad._
import scalaz.syntax.foldable._

trait RandomLibModule[M[+_]] extends ColumnarTableLibModule[M] {
  trait RandomLib extends ColumnarTableLib {
    import trans._

    val RandomNamespace = Vector("std", "random")

    override def _libMorphism1 = super._libMorphism1 ++ Set(UniformDistribution)

    object UniformDistribution extends Morphism1(RandomNamespace, "uniform") {
      // todo currently we are seeding with a number, change this to a String
      val tpe = UnaryOperationType(JNumberT, JNumberT)
      override val isInfinite = true

      type Result = Option[Long]
      
      def reducer(ctx: MorphContext) = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {
          val cols = schema.columns(JObjectFixedT(Map(paths.Value.name -> JNumberT)))

          val result: Set[Result] = cols map {
            case (c: LongColumn) => 
              range collectFirst { case i if c.isDefinedAt(i) => i } map { c(_) }

            case _ => None
          }

          if (result.isEmpty) None
          else result.suml(implicitly[Monoid[Result]])
        }
      }

      def extract(res: Result): Table = {
        res map { resultSeed =>
          val distTable = Table.uniformDistribution(MmixPrng(resultSeed))
          distTable.transform(buildConstantWrapSpec(TransSpec1.Id))
        } getOrElse Table.empty
      }

      def apply(table: Table, ctx: MorphContext): M[Table] =
        table.reduce(reducer(ctx)) map extract
    }
  }
}
