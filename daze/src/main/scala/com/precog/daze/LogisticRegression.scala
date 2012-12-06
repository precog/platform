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

import math.{exp, log, pow}

import scala.util.Random

import scala.annotation.tailrec

import scalaz._
import scalaz.std.anyVal._
import scalaz.std.option._
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

trait RegressionLib[M[+_]] extends GenOpcode[M] with ReductionLib[M] {
  import trans._

  val Stats2Namespace = Vector("std", "stats")

  override def _libMorphism2 = super._libMorphism2 ++ Set(LogisticRegression)

  object LogisticRegression extends Morphism2(Stats2Namespace, "logisticRegression") with ReductionHelper {
    val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

    override val multivariate = true
    lazy val alignment = MorphismAlignment.Match

    def sigmoid(z: Double): Double = 1 / (1 + exp(z))

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

    def dotProduct(xs: Array[Double], ys: Array[Double]): Double = {
      assert(xs.length == ys.length)
      var i = 0
      var result = 0.0
      while (i < xs.length) {
        result += xs(i) * ys(i)
        i += 1
      }
      result
    }    

    def cost(seq: Seq[ColumnValues], theta: Theta): Double = {
      val result = seq.foldLeft(0D) {
        case (sum, colVal) => {
          //TODO deal with taking last from empty seq
          val xs = colVal.take(colVal.length - 1)
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
    
    def gradient(seq: Seq[ColumnValues], theta: Theta, alpha: Double): Theta = { 
      seq.foldLeft(theta) { 
        case (theta, colVal) => {
          //TODO deal with taking last from empty seq
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
    
    def reduceDouble(seq0: Seq[ColumnValues]): Result = {
      if (seq0.isEmpty) {
        None
      } else {
        val seq = seq0 filter { arr => arr.last == 0 || arr.last == 1 }
        if (seq.isEmpty) None
        else Some(seq)
      }
    }

    def reducer: Reducer[Result] = new Reducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val features = cols(JArrayHomogeneousT(JNumberT))

        val result: Set[Result] = features map {
          case c: HomogeneousArrayColumn[Double] => 
            val mapped = range filter { r => c.isDefinedAt(r) } map { i => 1.0 +: c(i) }
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

      if (sum / theta.length < 0.0001) {
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
            val thetaLength = seq(0).length
            val thetas = Seq.fill(1000)(Array.fill(thetaLength - 1)(Random.nextGaussian * 10))

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

          //todo this is a hack?
          val tree = CPath.makeTree(cpaths, Range(1, finalTheta.length).toSeq :+ 0)

          val spec = TransSpec.concatChildren(tree)

          val theta = Table.constArray(Set(CArray[Double](finalTheta)))

          val result = theta.transform(spec)

          val valueTable = result.transform(trans.WrapObject(Leaf(Source), paths.Value.name))
          val keyTable = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

          valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
        }
      } getOrElse Table.empty
    }

    def apply(table: Table): M[Table] = {
      val schemas: M[Seq[JType]] = table.schemas map { _.toSeq }
      
      val specs: M[Seq[TransSpec1]] = schemas map {
        _ map { jtype => trans.Typed(TransSpec1.Id, jtype) }
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
        _.map { case (table, jtype) => tableReducer(table, jtype) }.toStream.sequence map( _.toSeq)
      }

      reducedTables map { _ reduceOption { _ concat _ } getOrElse Table.empty }
    }
  }
}
