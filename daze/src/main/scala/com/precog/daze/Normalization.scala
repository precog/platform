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
import yggdrasil.table._

import bytecode._
import common._

import scalaz._
import scalaz.syntax.monad._

trait NormalizationHelperModule[M[+_]] extends ColumnarTableLibModule[M] {
  trait NormalizationHelperLib extends ColumnarTableLib {
    trait NormalizationHelper {
      val tpe = BinaryOperationType(JType.JUniverseT, JObjectUnfixedT, JType.JUniverseT)
    
      case class Stats(mean: Double, stdDev: Double)
      type Summary = Map[CPath, Stats]
    
      implicit val monoid = new Monoid[Summary] {
        def zero = Map.empty[CPath, Stats]
        def append(left: Summary, right: => Summary): Summary = sys.error("actually we should never reach this case unless we have a slice worth of summaries?")
      }
    
      def reducer = new CReducer[Summary] {
        def reduce(schema: CSchema, range: Range) = sys.error("todo")
      }
    
      def morph1Apply(summary: Summary): Morph1Apply
    
      lazy val alignment = MorphismAlignment.Custom(alignCustom _)
    
      def alignCustom(t1: Table, t2: Table): M[(Table, Morph1Apply)] = {
        t2.reduce(reducer) map { summary => (t1, morph1Apply(summary)) }
      }
    }
  }
}

trait NormalizationModule[M[+_]] extends NormalizationHelperModule[M] {
  trait NormalizationLib extends NormalizationHelperLib {

    object Normalization extends Morphism2(Vector("std", "stats"), "normalize") with NormalizationHelper {
      override val retainIds = true

      def morph1Apply(summary: Summary) = new Morph1Apply {
        def apply(table: Table, ctx: EvaluationContext): M[Table] = sys.error("todo")
      }
    }
  }
}

trait DenormalizationModule[M[+_]] extends NormalizationHelperModule[M] {
  trait DenormalizationLib extends NormalizationHelperLib {

    object Denormalization extends Morphism2(Vector("std", "stats"), "denormalize") with NormalizationHelper {
      override val retainIds = true

      def morph1Apply(summary: Summary) = new Morph1Apply {
        def apply(table: Table, ctx: EvaluationContext): M[Table] = sys.error("todo")
      }
    }
  }
}
