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
