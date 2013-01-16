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

import com.precog.bytecode._

trait PredictionLib[M[+_]] extends GenOpcode[M] {
  override def _libMorphism2 = super._libMorphism2 ++ Set(LinearPrediction)

  val Stats3Namespace = Vector("std", "stats")

  object LinearPrediction extends Morphism2(Stats3Namespace, "predictLinear") {
    val tpe = BinaryOperationType(JObjectUnfixedT, JType.JUniverseT, JObjectUnfixedT)

    override val multivariate = false
    lazy val alignment = MorphismAlignment.Cross  //really need something that's neight Cross nor Match

    def apply(table: Table, ctx: EvaluationContext) = sys.error("todo")
  }
}
