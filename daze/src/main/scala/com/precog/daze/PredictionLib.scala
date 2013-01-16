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
