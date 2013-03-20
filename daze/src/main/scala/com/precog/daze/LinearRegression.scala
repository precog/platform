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
import common.json._

import bytecode._
import TableModule._

import Jama._
import Jama.Matrix._

import blueeyes.json._

import scalaz._
import scalaz.syntax.monad._
import scalaz.std.list._
import scalaz.std.stream._
import scalaz.std.set._
import scalaz.syntax.traverse._

trait LinearRegressionLibModule[M[+_]] 
    extends ColumnarTableLibModule[M]
    with ReductionLibModule[M]
    with EvaluatorMethodsModule[M]
    with PredictionLibModule[M] {

  trait LinearRegressionLib 
      extends ColumnarTableLib
      with RegressionSupport
      with PredictionSupport
      with ReductionLib
      with EvaluatorMethods {

    import trans._

    override def _libMorphism2 = super._libMorphism2 ++ Set(MultiLinearRegression, LinearPrediction)

    object MultiLinearRegression extends Morphism2(Stats2Namespace, "linearRegression") {
      val tpe = BinaryOperationType(JNumberT, JType.JUniverseT, JObjectUnfixedT)

      lazy val alignment = MorphismAlignment.Match(M.point(morph1))

      type Beta = Array[Double]

      case class CoeffAcc(beta: Beta, count: Long)

      // `rss`: Residual Sum of Squares
      // `tss`: Total Sum of Squares
      case class StdErrorAcc(rss: Double, tss: Double, product: Matrix)

      /**
       * http://adrem.ua.ac.be/sites/adrem.ua.ac.be/files/StreamFitter.pdf
       *
       * First slice size is made consistent. Then the slices are fed to the reducer, one by one.
       * The reducer calculates the Ordinary Least Squares regression for each Slice. 
       * The results of each of these regressions are then combined incrementally using `monoid`. 
       * `alpha` (a value between 0 and 1) is the paramater which determines the weighting of the
       * data in the stream. A value of 0.5 means that current values and past values
       * are equally weighted. The paper above outlines how `alpha` relates to the half-life
       * of the current window (i.e. the current Slice). In the future, we could let half-life,
       * or something related, be an optional parameter in the regression model.
       */


      /**
       * The following code will necessary when we have online models and users want to weight
       * the most recent data with a higher (or lower) weight than the data already seen.
       * But since we don't have this capability yet, all data is weighted equally.

      val alpha = 0.5

      implicit def monoid = new Monoid[CoeffAcc] {
        def zero = Array.empty[Double]
        def append(r1: CoeffAcc, r2: => CoeffAcc) = {
          lazy val newr1 = r1 map { _ * alpha }
          lazy val newr2 = r2 map { _ * (1.0 - alpha) }

          if (r1.isEmpty) r2
          else if (r2.isEmpty) r1
          else arraySum(newr1, newr2)
        }
      }
      */

      implicit def coeffMonoid = new Monoid[CoeffAcc] {
        def zero = CoeffAcc(Array.empty[Double], 0)
        def append(r1: CoeffAcc, r2: => CoeffAcc) = {
          if (r1.beta isEmpty) r2
          else if (r2.beta isEmpty) r1
          else CoeffAcc(arraySum(r1.beta, r2.beta), r1.count + r2.count)
        }
      }

      implicit def stdErrorMonoid = new Monoid[StdErrorAcc] {
        def zero = StdErrorAcc(0, 0, new Matrix(Array(Array.empty[Double])))

        def append(t1: StdErrorAcc, t2: => StdErrorAcc) = {
          def isEmpty(matrix: Matrix) = {
            // there has to be a better way...
            matrix.getArray.length == 1 && matrix.getArray.head.length == 0
          }

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

          StdErrorAcc(t1.rss + t2.rss, t1.tss + t2.tss, matrixSum)
        }
      }

      implicit def betaMonoid = new Monoid[Option[Array[Beta]]] {
        def zero = None
        def append(t1: Option[Array[Beta]], t2: => Option[Array[Beta]]) = {
          t1 match {
            case None => t2
            case Some(c1) => t2 match {
              case None => Some(c1)
              case Some(c2) => Some(c1 ++ c2)
            }
          }
        }
      }

      def coefficientReducer: Reducer[CoeffAcc] = new Reducer[CoeffAcc] {
        def reduce(schema: CSchema, range: Range): CoeffAcc = {
          val features = schema.columns(JArrayHomogeneousT(JNumberT))

          val count = {
            var countAcc = 0L
            RangeUtil.loop(range) { i =>
              if (Column.isDefinedAt(features.toArray, i)) countAcc += 1L
            }
            countAcc
          }

          val values: Set[Option[Array[Array[Double]]]] = features map {
            case c: HomogeneousArrayColumn[_] if c.tpe.manifest.erasure == classOf[Array[Double]] =>
              val mapped = range.toArray filter { r => c.isDefinedAt(r) } map { i => c.asInstanceOf[HomogeneousArrayColumn[Double]](i) }
              Some(mapped)
            case other => 
              logger.warn("Features were not correctly put into a homogeneous array of doubles; returning empty.")
              None
          }

          val arrays = {
            if (values.isEmpty) None
            else values.suml(betaMonoid)
          }

          val xs = arrays map { _ map { arr => 1.0 +: (java.util.Arrays.copyOf(arr, arr.length - 1)) } }
          val y0 = arrays map { _ map { _.last } }

          val matrixX = xs map { case arr => new Matrix(arr) }

          // FIXME ultimately we do not want to throw an IllegalArgumentException here
          // once the framework is in place, we will return the empty set and issue a warning to the user
          // this catches the case when the user runs regression on data when rows < (columns + 1)
          val inverseX = try {
            matrixX map { _.inverse() }
          } catch {
            case ex: RuntimeException if ex.getMessage == "Matrix is rank deficient." => 
              throw new IllegalArgumentException("More features than rows found in linear regression. Not enough information to determine model.", ex)
          }

          val matrixY = y0 map { case arr => new Matrix(Array(arr)) }

          val matrixProduct: Option[Matrix] = for {
            inverse <- inverseX
            y <- matrixY
          } yield {
            (inverse).times(y.transpose)
          }

          val res = matrixProduct map { _.getArray flatten } getOrElse Array.empty[Double]

          // We weight the results to handle slices of different sizes.
          // Even though we canonicalize the slices, the last slice may be smaller 
          // than all the others.
          val weightedRes = res map { _ * count }
          
          CoeffAcc(weightedRes, count)
        }
      }

      def stdErrorReducer(acc: CoeffAcc): Reducer[StdErrorAcc] = new Reducer[StdErrorAcc] {
        def reduce(schema: CSchema, range: Range): StdErrorAcc = {
          val features = schema.columns(JArrayHomogeneousT(JNumberT))

          val values: Set[Option[Array[Array[Double]]]] = features map {
            case c: HomogeneousArrayColumn[_] if c.tpe.manifest.erasure == classOf[Array[Double]] =>
              val mapped = range.toArray filter { r => c.isDefinedAt(r) } map { i => c.asInstanceOf[HomogeneousArrayColumn[Double]](i) }
              Some(mapped)
            case other => 
              logger.warn("Features were not correctly put into a homogeneous array of doubles; returning empty.")
              None
          }

          val arrays = {
            if (values.isEmpty) None
            else values.suml(betaMonoid)
          }

          val xs = arrays map { _ map { arr => 1.0 +: (java.util.Arrays.copyOf(arr, arr.length - 1)) } }
          val y0 = arrays map { _ map { _.last } }

          val matrixX = xs map { case arr => new Matrix(arr) }
          val matrixProduct = matrixX map { matrix => matrix.transpose() times matrix } getOrElse { stdErrorMonoid.zero.product }

          val actualY0 = y0 map { arr => (new Matrix(Array(arr))).transpose() }
          val weightedBeta = acc.beta map { _ / acc.count }
          val predictedY0 = matrixX map { _.times((new Matrix(Array(weightedBeta))).transpose()) }

          val rssOpt = for {
            actualY <- actualY0
            predictedY <- predictedY0
          } yield {
            val difference = actualY.minus(predictedY)
            val prod = difference.transpose() times difference

            assert(prod.getRowDimension == 1 && prod.getColumnDimension == 1)
            prod.getArray.head.head
          }

          val rss = rssOpt getOrElse { stdErrorMonoid.zero.rss }

          val yMean = y0.flatten.sum / y0.flatten.size

          val tss = y0.flatten map { y => math.pow(y - yMean, 2d) } sum

          StdErrorAcc(rss, tss, matrixProduct)
        }
      }

      def extract(coeffs: CoeffAcc, errors: StdErrorAcc, jtype: JType): Table = {
        val cpaths = Schema.cpath(jtype)

        val tree = CPath.makeTree(cpaths, Range(1, coeffs.beta.length).toSeq :+ 0)

        val spec = TransSpec.concatChildren(tree)

        val weightedBeta = coeffs.beta map { _ / coeffs.count }

        val colDim = errors.product.getColumnDimension

        val varianceEst = errors.rss / (coeffs.count - colDim)
        
        val inverse = errors.product.inverse()

        val stdErrors = (0 until colDim) map { case i => math.sqrt(varianceEst * inverse.get(i, i)) }

        assert(weightedBeta.length == stdErrors.length)

        val resultCoeffs = weightedBeta.map(CNum(_))
        val resultErrors = stdErrors.map(CNum(_))

        val arr = resultCoeffs.zip(resultErrors) map { case (beta, error) =>
          RObject(Map("estimate" -> beta, "standardError" -> error))
        }

        val theta = Table.fromRValues(Stream(RArray(arr.toList)))

        val thetaInSchema = theta.transform(spec)

        val coeffsTable = thetaInSchema.transform(trans.WrapObject(Leaf(Source), "coefficients"))

        val rSquared = 1 - (errors.rss / errors.tss)
        val rSquaredTable0 = Table.fromRValues(Stream(CNum(rSquared)))
        val rSquaredTable = rSquaredTable0.transform(trans.WrapObject(Leaf(Source), "RSquared"))

        val result = coeffsTable.cross(rSquaredTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))

        val valueTable = result.transform(trans.WrapObject(Leaf(Source), paths.Value.name))
        val keyTable = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

        valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
      }

      private val morph1 = new Morph1Apply {
        def apply(table0: Table, ctx: EvaluationContext) = {
          val ySpec0 = DerefArrayStatic(TransSpec1.Id, CPathIndex(0))
          val xsSpec0 = DerefArrayStatic(TransSpec1.Id, CPathIndex(1))

          val ySpec = trans.Map1(ySpec0, cf.util.CoerceToDouble)
          val xsSpec = trans.DeepMap1(xsSpec0, cf.util.CoerceToDouble)

          // `arraySpec` generates the schema in which the Coefficients will be returned
          val arraySpec = InnerArrayConcat(trans.WrapArray(xsSpec), trans.WrapArray(ySpec))
          val table = table0.transform(arraySpec)

          val schemas: M[Seq[JType]] = table.schemas map { _.toSeq }
          
          val specs: M[Seq[TransSpec1]] = schemas map {
            _ map { jtype => trans.Typed(TransSpec1.Id, jtype) }
          }

          val tables: M[Seq[Table]] = specs map { _ map { table.transform } }

          val tablesWithType: M[Seq[(Table, JType)]] = for {
            tbls <- tables
            jtypes <- schemas
          } yield {
            tbls zip jtypes
          }

          // important note: regression will explode if there are more than 1000 columns due to rank-deficient matrix
          // this could be remedied in the future by smarter choice of `sliceSize`
          // though do we really want to allow people to run regression on >1000 columns?
          val sliceSize = 1000
          val tableReducer: (Table, JType) => M[Table] = {
            (table, jtype) => {
              val arrayTable = table.canonicalize(sliceSize).toArray[Double].normalize

              val coeffs0 = arrayTable.reduce(coefficientReducer)
              val errors0 = coeffs0 flatMap { acc => arrayTable.reduce(stdErrorReducer(acc)) }

              for {
                coeffs <- coeffs0
                errors <- errors0
              } yield {
                extract(coeffs, errors, jtype)
              }
            }
          }

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

          objectTables map { _.reduceOption {
            (tl, tr) => tl.cross(tr)(buildConstantWrapSpec(spec))
          } getOrElse Table.empty }
        }
      }
    }

    object LinearPrediction extends Morphism2(Stats2Namespace, "predictLinear") with PredictionBase {
      val tpe = BinaryOperationType(JType.JUniverseT, JObjectUnfixedT, JObjectUnfixedT)

      override val retainIds = true

      lazy val alignment = MorphismAlignment.Custom(alignCustom _)

      def alignCustom(t1: Table, t2: Table): M[(Table, Morph1Apply)] = {
        val spec = liftToValues(trans.DeepMap1(TransSpec1.Id, cf.util.CoerceToDouble))
        t2.transform(spec).reduce(reducer) map { models =>
          (t1.transform(spec), morph1Apply(models, scala.Predef.identity[Double]))
        }
      }
    }
  }
}
