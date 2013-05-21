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
import TableModule._

import math.{exp, log, pow}

import scala.util.Random

import scala.annotation.tailrec

import blueeyes.json._

import scalaz._
import scalaz.std.anyVal._
import scalaz.std.option._
import scalaz.std.list._
import scalaz.std.set._
import scalaz.std.stream._
import scalaz.std.tuple._
import scalaz.std.function._
import scalaz.syntax.foldable._
import scalaz.syntax.traverse._
import scalaz.syntax.monad._

trait ReductionHelper {
  type Result
  type ColumnValues
  def reduceDouble(seq: Seq[ColumnValues]): Result
}

trait LogisticRegressionLibModule[M[+_]] extends ColumnarTableLibModule[M] with ReductionLibModule[M] with EvaluatorMethodsModule[M] with PredictionLibModule[M] {
  trait LogisticRegressionLib extends ColumnarTableLib with ReductionLib with PredictionSupport with RegressionSupport with EvaluatorMethods {
    import trans._

    override def _libMorphism2 = super._libMorphism2 ++ Set(LogisticRegression, LogisticPrediction)

    object LogisticRegression extends Morphism2(Stats2Namespace, "logisticRegression") with ReductionHelper {
      val tpe = BinaryOperationType(JNumberT, JType.JUniverseT, JObjectUnfixedT)

      lazy val alignment = MorphismAlignment.Match(M.point(morph1))

      def sigmoid(z: Double): Double = 1.0 / (1.0 + exp(z))

      type Theta = Array[Double]
      type ColumnValues = Array[Double]
      type Result = Option[Seq[ColumnValues]]

      implicit def monoid = new Monoid[Result] {
        def zero = None
        def append(t1: Option[Seq[ColumnValues]], t2: => Option[Seq[ColumnValues]]) = {
          t1 match {
            case None => t2
            case Some(c1) => t2 match {
              case None => Some(c1)
              case Some(c2) => Some(c1 ++ c2)
            }
          }
        }
      }

      def checkValue(value: Double): Double = {
        if (value isPosInfinity)
          Double.MaxValue
        else if (value isNegInfinity)
          Double.MinValue
        else if (value isNaN)
          sys.error("Inconceivable! Value is NaN.")
        else
          value
      }

      def cost(seq: Seq[ColumnValues], theta: Theta): Double = {
        if (seq.isEmpty) {
          sys.error("empty sequence should never occur")
        } else {
          val result = seq.foldLeft(0D) {
            case (sum, colVal) => {
              val xs = java.util.Arrays.copyOf(colVal, colVal.length - 1)
              val y = colVal.last

              assert(xs.length == theta.length)

              if (y == 1) {
                val result = log(sigmoid(dotProduct(theta, xs)))

                sum + checkValue(result)
              } else if (y == 0) {
                val result = log(1 - sigmoid(dotProduct(theta, xs)))

                sum + checkValue(result)
              } else {
                sys.error("unreachable case")
              }
            }
          }
      
          -result
        }
      }
      
      def gradient(seq: Seq[ColumnValues], theta: Theta, alpha: Double): Theta = { 
        if (seq.isEmpty) {
          sys.error("empty sequence should never occur")
        } else {
          seq.foldLeft(theta) { 
            case (theta, colVal) => {
              val xs = colVal.take(colVal.length - 1)
              val y = colVal.last

              assert(xs.length == theta.length)

              val result = (0 until xs.length).map { i =>
                theta(i) - alpha * (y - sigmoid(dotProduct(theta, xs))) * xs(i)
              }.map(checkValue)

              result.toArray
            }
          }
        }
      }
      
      def reduceDouble(seq0: Seq[ColumnValues]): Result = {
        val seq = seq0 filter { arr => arr.last == 0 || arr.last == 1 }

        if (seq.isEmpty) None
        else Some(seq)
      }

      def reducer: Reducer[Result] = new Reducer[Result] {
        def reduce(schema: CSchema, range: Range): Result = {
          val features = schema.columns(JArrayHomogeneousT(JNumberT))

          val result: Set[Result] = features map {
            case c: HomogeneousArrayColumn[_] if c.tpe.manifest.erasure == classOf[Array[Double]] =>
              val mapped = range filter { r => c.isDefinedAt(r) } map { i => 1.0 +: c.asInstanceOf[HomogeneousArrayColumn[Double]](i) }
              reduceDouble(mapped)
            case _ => None
          }

          if (result.isEmpty) None
          else result.suml(monoid)
        }
      }

      @tailrec
      def gradloop(seq: Seq[ColumnValues], theta0: Theta, alpha: Double): Theta = {
        val theta = gradient(seq, theta0, alpha)
        
        val diffs = theta0.zip(theta) map { case (t0, t) => math.abs(t0 - t) }
        val sum = diffs.sum

        if (sum / theta.length < 0.01) {
          theta
        } else if (cost(seq, theta) > cost(seq, theta0)) {
          if (alpha > Double.MinValue * 2.0)
            gradloop(seq, theta0, alpha / 2.0)
          else 
            theta0
        } else { 
          gradloop(seq, theta, alpha)
        }
      }

      def extract(res: Result, jtype: JType): Table = {
        val cpaths = Schema.cpath(jtype)

        res map {
          case seq => {
            val initialTheta: Theta = {
              val thetaLength = seq.headOption map { _.length } getOrElse sys.error("unreachable: `res` would have been None")
              val thetas = Seq.fill(100)(Array.fill(thetaLength - 1)(Random.nextGaussian * 10))

              val (result, _) = (thetas.tail).foldLeft((thetas.head, cost(seq, thetas.head))) {
                case ((theta0, cost0), theta) => {
                  val costnew = cost(seq, theta)

                  if (costnew < cost0)
                    (theta, costnew)
                  else
                    (theta0, cost0)
                }
              }

              result
            }

            val initialAlpha = 1.0

            val finalTheta: Theta = gradloop(seq, initialTheta, initialAlpha)

            val tree = CPath.makeTree(cpaths, Range(1, finalTheta.length).toSeq :+ 0)

            val spec = TransSpec.concatChildren(tree)

            val res = finalTheta map { v =>
              RObject(Map("estimate" -> CNum(v)))
            }

            val theta = Table.fromRValues(Stream(RArray(res.toList)))

            val result = theta.transform(spec)

            val coeffsTable = result.transform(trans.WrapObject(Leaf(Source), "coefficients"))

            val valueTable = coeffsTable.transform(trans.WrapObject(Leaf(Source), paths.Value.name))
            val keyTable = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

            valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
          }
        } getOrElse Table.empty
      }

      private val morph1 = new Morph1Apply {
        def apply(table0: Table, ctx: EvaluationContext): M[Table] = {
          val ySpec0 = DerefArrayStatic(TransSpec1.Id, CPathIndex(0))
          val xsSpec0 = DerefArrayStatic(TransSpec1.Id, CPathIndex(1))

          val ySpec = trans.Map1(ySpec0, cf.util.CoerceToDouble)
          val xsSpec = trans.DeepMap1(xsSpec0, cf.util.CoerceToDouble)

          // `arraySpec` generates the schema in which the Coefficients will be returned
          val arraySpec = InnerArrayConcat(trans.WrapArray(xsSpec), trans.WrapArray(ySpec))
          val table = table0.transform(arraySpec)

          val schemas: M[Seq[JType]] = table.schemas map { _.toSeq }
          
          val specs: M[Seq[TransSpec1]] = schemas map {
            _ map { jtype => trans.Typed(trans.DeepMap1(TransSpec1.Id, cf.util.CoerceToDouble) , jtype) }
          }

          val sampleTables: M[Seq[Table]] = specs flatMap { seq => table.sample(10000, seq) }

          val tablesWithType: M[Seq[(Table, JType)]] = for {
            samples <- sampleTables
            jtypes <- schemas
          } yield {
            samples zip jtypes
          }

          val tableReducer: (Table, JType) => M[Table] = 
            (table, jtype) => table.toArray[Double].reduce(reducer).map(res => extract(res, jtype))

          val reducedTables: M[Seq[Table]] = tablesWithType flatMap { 
            _.map { case (table, jtype) => tableReducer(table, jtype) }.toStream.sequence map(_.toSeq)
          }

          val objectTables: M[Seq[Table]] = reducedTables map { 
            _.zipWithIndex map { case (tbl, idx) =>
              val modelId = "model" + (idx + 1)
              tbl.transform(liftToValues(trans.WrapObject(TransSpec1.Id, modelId)))
            }
          }

          val spec = OuterObjectConcat(
            DerefObjectStatic(Leaf(SourceLeft), paths.Value),
            DerefObjectStatic(Leaf(SourceRight), paths.Value))

          objectTables map { _.reduceOption { (tl, tr) => tl.cross(tr)(buildConstantWrapSpec(spec)) } getOrElse Table.empty }
        }
      }
    }

    object LogisticPrediction extends Morphism2(Stats2Namespace, "predictLogistic") with LogisticPredictionBase {
      val tpe = BinaryOperationType(JType.JUniverseT, JObjectUnfixedT, JObjectUnfixedT)

      override val idPolicy = IdentityPolicy.Retain.Merge

      lazy val alignment = MorphismAlignment.Custom(IdentityAlignment.CrossAlignment, alignCustom _)

      def alignCustom(t1: Table, t2: Table): M[(Table, Morph1Apply)] = {
        val spec = liftToValues(trans.DeepMap1(TransSpec1.Id, cf.util.CoerceToDouble))
        def sigmoid(d: Double): Double = 1.0 / (1.0 + math.exp(d))

        t2.transform(spec).reduce(reducer) map { models => (t1.transform(spec), morph1Apply(models, sigmoid _)) }
      }
    }
  }
}
