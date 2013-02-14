package com.precog
package daze

import bytecode._

import yggdrasil._
import yggdrasil.table._
import TransSpecModule._

import com.precog.util.{BitSet, BitSetUtil}
import com.precog.util.BitSetUtil.Implicits._
import com.precog.common.json._

import scalaz._
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.syntax.monad._
import scalaz.syntax.foldable._

trait RandomLibModule[M[+_]] extends ColumnarTableLibModule[M] with EvaluatorMethodsModule[M] {
  trait RandomLib extends ColumnarTableLib with EvaluatorMethods {
    import trans._

    val RandomNamespace = Vector("std", "random")

    override def _libMorphism1 = super._libMorphism1 ++ Set(UniformDistribution)

    object UniformDistribution extends Morphism1(RandomNamespace, "uniform") {
      val tpe = UnaryOperationType(JNumberT, JNumberT)  //todo input should be JTextT

      type Result = Option[Long]
      
      implicit def monoid = implicitly[Monoid[Result]]

      def reducer(ctx: EvaluationContext) = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {
          val cols = schema.columns(JObjectFixedT(Map(paths.Value.name -> JNumberT)))

          val result: Set[Result] = cols map {
            case (c: LongColumn) => 
              range collectFirst { case i if c.isDefinedAt(i) => i } map { c(_) }

            case _ => None
          }

          if (result.isEmpty) None
          else result.suml(monoid)
        }
      }

      def extract(res: Result): Table = {
        res map { resultSeed =>
          val distTable = Table.uniformDistribution(MmixPrng(resultSeed))
          distTable.transform(buildConstantWrapSpec(TransSpec1.Id))
        } getOrElse Table.empty
      }

      def apply(table: Table, ctx: EvaluationContext): M[Table] =
        table.reduce(reducer(ctx)) map extract
    }
  }
}
