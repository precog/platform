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

import common._
import bytecode._
import util._

import scalaz._
import scalaz.std.anyVal._
import scalaz.std.map._
import scalaz.std.stream._
import scalaz.syntax.traverse._
import scalaz.syntax.monad._

trait SummaryLibModule[M[+_]] extends ReductionLibModule[M] with EvaluatorMethodsModule[M] {
  trait SummaryLib extends ReductionLib with EvaluatorMethods {
    import trans._
    import TransSpecModule._

    override def _libMorphism1 = super._libMorphism1 ++ Set(Summary)

    val reductions = List(Mean, StdDev, Min, Max, Count)
    val coalesced: Reduction = coalesce(reductions.map(_ -> None))

    val reductionSpecs: List[TransSpec1] = reductions.reverse.zipWithIndex map { case (red, idx) =>
      trans.WrapObject(trans.DerefArrayStatic(TransSpec1.Id, CPathIndex(idx)), red.name)
    }
    val reductionSpec = reductionSpecs reduce { trans.OuterObjectConcat(_, _) }

    object SingleSummary extends Reduction(Vector(), "singleSummary") {
      val tpe = coalesced.tpe

      type Result = coalesced.Result
      val monoid: Monoid[Result] = coalesced.monoid

      def reducer(ctx: EvaluationContext): CReducer[Result] = coalesced.reducer(ctx)

      def extract(res: Result): Table = {
        val arrayTable = coalesced.extract(res)
        arrayTable.transform(reductionSpec)
      }

      def extractValue(res: Result): Option[RValue] = coalesced.extractValue(res)
    }

    object Summary extends Morphism1(Vector(), "summary") {
      val tpe = UnaryOperationType(JType.JUniverseT, JObjectUnfixedT)

      override val idPolicy: IdentityPolicy = IdentityPolicy.Strip

      def makeReduction(jtpe: JType): Reduction = {
        val jtypes: List[Option[JType]] = {
          val grouped = Schema.flatten(jtpe, List.empty[ColumnRef]).groupBy(_._1)
          val numerics = grouped filter { case (cpath, pairs) =>
            pairs.map(_._2).exists(_.isNumeric)
          }

          // handles case when we have multiple numeric columns at same path
          val singleNumerics = numerics.toList.map { case (path, _) => (path, CNum) }
          val sortedNumerics = singleNumerics.distinct.sortBy(_._1).reverse

          sortedNumerics map { case (cpath, ctype) =>
            Schema.mkType(Seq(cpath -> ctype))
          }
        }

        val functions: List[Option[JType => JType]] =
          jtypes.distinct map ( _ map { Schema.replaceLeaf } )

        coalesce(functions map { SingleSummary -> _ })
      }

      def reduceTable(table: Table, jtype: JType, ctx: EvaluationContext): M[Table] = {
        val reduction = makeReduction(jtype)

        implicit def monoid = reduction.monoid

        val values = table.reduce(reduction.reducer(ctx))

        def extract(result: reduction.Result, jtype: JType): Table = {
          val paths = Schema.cpath(jtype)

          val tree = CPath.makeTree(paths, 0 until paths.length)
          val spec = TransSpec.concatChildren(tree)

          reduction.extract(result).transform(spec)
        }

        values map { extract(_, jtype) }
      }

      def apply(table: Table, ctx: EvaluationContext) = {
        val jtypes0: M[Seq[Option[JType]]] = for {
          schemas <- table.schemas
        } yield {
          schemas.toSeq map { jtype =>
            val flattened = Schema.flatten(jtype, List.empty[ColumnRef])

            val values = flattened filter { case (cpath, ctype) =>
              cpath.hasPrefix(paths.Value) && ctype.isNumeric
            }

            Schema.mkType(values)
          }
        }

        // one JType-with-numeric-leaves per schema
        val jtypes: M[Seq[JType]] = jtypes0 map ( _ collect {
          case opt if opt.isDefined => opt.get
        })

        val specs: M[Seq[TransSpec1]] = jtypes map {
          _ map { trans.Typed(TransSpec1.Id, _) }
        }

        // one table per schema
        val tables: M[Seq[Table]] = specs map ( _ map { spec =>
          table.transform(spec).compact(TransSpec1.Id, AllDefined)
        })

        val tablesWithType: M[Seq[(Table, JType)]] = for {
          tbls <- tables
          schemas <- jtypes
        } yield {
          tbls zip schemas
        }

        val resultTables: M[Seq[Table]] = tablesWithType flatMap {
          _.map { case (table, jtype) =>
            reduceTable(table, jtype, ctx)
          }.toStream.sequence map(_.toSeq)
        }

        val objectTables: M[Seq[Table]] = resultTables map {
          _.zipWithIndex map { case (tbl, idx) =>
            val modelId = "model" + (idx + 1)
            tbl.transform(trans.WrapObject(DerefObjectStatic(TransSpec1.Id, paths.Value), modelId))
          }
        }

        val spec = OuterObjectConcat(Leaf(SourceLeft), Leaf(SourceRight))

        val res = objectTables map { _.reduceOption {
          (tl, tr) => tl.cross(tr)(spec)
        } getOrElse Table.empty }

        res map { _.transform(buildConstantWrapSpec(TransSpec1.Id)) }
      }
    }
  }
}
