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

import common.json._

import bytecode._
import TableModule._

import Jama._
import Jama.Matrix._

import scalaz._
import scalaz.syntax.monad._
import scalaz.std.stream._
import scalaz.std.set._
import scalaz.syntax.traverse._

trait LinearRegressionLib[M[+_]] extends GenOpcode[M] with RegressionSupport {
  import trans._

  override def _libMorphism2 = super._libMorphism2 ++ Set(MultiLinearRegression)

  object MultiLinearRegression extends Morphism2(Stats2Namespace, "linearRegression") {
    val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

    override val multivariate = true
    lazy val alignment = MorphismAlignment.Match

    type Beta = Array[Double]
    type Result = Beta

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

    val alpha = 0.5

    implicit def monoid = new Monoid[Result] {
      def zero = Array.empty[Double]
      def append(r1: Result, r2: => Result) = {
        lazy val newr1 = r1 map { _ * alpha }
        lazy val newr2 = r2 map { _ * (1.0 - alpha) }

        if (r1.isEmpty) r2
        else if (r2.isEmpty) r1
        else arraySum(newr1, newr2)
      }
    }

    implicit def resultMonoid = new Monoid[Option[Array[Result]]] {
      def zero = None
      def append(t1: Option[Array[Result]], t2: => Option[Array[Result]]) = {
        t1 match {
          case None => t2
          case Some(c1) => t2 match {
            case None => Some(c1)
            case Some(c2) => Some(c1 ++ c2)
          }
        }
      }
    }

    def reducer: Reducer[Result] = new Reducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val features = cols(JArrayHomogeneousT(JNumberT))

        val values: Set[Option[Array[Array[Double]]]] = features map {
          case c: HomogeneousArrayColumn[_] if c.tpe.manifest.erasure == classOf[Array[Double]] =>
            val mapped = range.toArray filter { r => c.isDefinedAt(r) } map { i => 1.0 +: c.asInstanceOf[HomogeneousArrayColumn[Double]](i) }
            Some(mapped)
          case other => 
            logger.warn("Features were not correctly put into a homogeneous array of doubles; returning empty.")
            None
        }

        val arrays = {
          if (values.isEmpty) None
          else values.suml(resultMonoid)
        }

        val xs = arrays map { _ map { arr => java.util.Arrays.copyOf(arr, arr.length - 1) } }
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

        matrixProduct map { _.getArray flatten } getOrElse Array.empty[Double]
      }
    }

    def extract(res: Result, jtype: JType): Table = {
      val cpaths = Schema.cpath(jtype)

      val tree = CPath.makeTree(cpaths, Range(1, res.length).toSeq :+ 0)

      val spec = TransSpec.concatChildren(tree)

      val theta = Table.constArray(Set(CArray[Double](res)))

      val result = theta.transform(spec)

      val valueTable = result.transform(trans.WrapObject(Leaf(Source), paths.Value.name))
      val keyTable = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

      valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
    }

    def apply(table: Table, ctx: EvaluationContext) = {
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
      val tableReducer: (Table, JType) => M[Table] =
        (table, jtype) => table.canonicalize(sliceSize).toArray[Double].normalize.reduce(reducer).map(res => extract(res, jtype))

      val reducedTables: M[Seq[Table]] = tablesWithType flatMap { 
        _.map { case (table, jtype) => tableReducer(table, jtype) }.toStream.sequence map(_.toSeq)
      }

      reducedTables map { _ reduceOption { _ concat _ } getOrElse Table.empty }
    }
  }
}
