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
import scalaz.std.tuple._
import scalaz.std.function._
import scalaz.syntax.foldable._
import scalaz.syntax.monad._

trait ReductionHelper {
  type Result
  def reduceDouble(seq: Seq[(Double, Double)]): Result
}


trait RegressionLib[M[+_]] extends GenOpcode[M] with ReductionLib[M] {
  import trans._

  val Stats2Namespace = Vector("std", "stats")

  override def _libMorphism2 = super._libMorphism2 ++ Set(LogisticRegression)


  object LogisticRegression extends Morphism2(Stats2Namespace, "logisticRegression") with ReductionHelper {
    val tpe = BinaryOperationType(JNumberT, JNumberT, JNumberT)

    lazy val alignment = MorphismAlignment.Match

    def sigmoid(z: Double): Double = 1 / (1 + exp(z))

    type Theta = Array[Double]
    type Result = Option[Seq[(Double, Double)]]

    implicit def monoid = new Monoid[Result] {
      def zero = None
      def append(t1: Option[Seq[(Double, Double)]], t2: => Option[Seq[(Double, Double)]]) = {
        t1 match {
          case None => t2
          case Some(c1) => t2 match {
            case None => Some(c1)
            case Some(c2) => Some(c1 ++ c2)
          }
        }
      }
    }

    def cost(seq: Seq[(Double, Double)], theta: Theta): Double = {
      val result = seq.foldLeft(0D) {
        case (sum, (x, y)) => {
          if (y == 1) {
            val result = log(sigmoid(theta(0) + theta(1) * x))
    
            if (result isPosInfinity)
              sum + Double.MaxValue
            else if (result isNegInfinity)
              sum + Double.MinValue
            else if (result isNaN)
              sys.error("explosion")
            else
              sum + result
          }
          else if (y == 0) {
            val result = log(1 - sigmoid(theta(0) + theta(1) * x))
    
            if (result isPosInfinity)
              sum + Double.MaxValue
            else if (result isNegInfinity)
              sum + Double.MinValue
            else if (result isNaN)
              sys.error("explosion")
            else
              sum + result
          }
          else
            sum
        }
      }
    
      -result
    }
    
    def gradient(seq: Seq[(Double, Double)], theta: Theta, alpha: Double): Theta = { 
      seq.foldLeft(theta) { 
        case (theta, (x, y)) => {
          val theta0 = theta(0) - alpha * (y - sigmoid(theta(0) + theta(1) * x)) * 1
          val theta1 = theta(1) - alpha * (y - sigmoid(theta(0) + theta(1) * x)) * x
    
          val newtheta0 = {
            if (theta0 isPosInfinity) 
              Double.MaxValue
            else if (theta0 isNegInfinity)
              Double.MinValue
            else if (theta0 isNaN)
              sys.error("explosion")
            else
              theta0
          }
    
          val newtheta1 = {
            if (theta1 isPosInfinity) 
              Double.MaxValue
            else if (theta1 isNegInfinity)
              Double.MinValue
            else if (theta1 isNaN)
              sys.error("explosion")
            else
              theta1
          }
    
          Array(newtheta0, newtheta1)
        }
      }
    }
    
    def reduceDouble(seq: Seq[(Double, Double)]): Result = {
      if (seq.isEmpty) {
        None
      } else {
        Some(seq)
      }
    }

    def reducer: Reducer[Result] = new Reducer[Result] {
      def reduce(cols: JType => Set[Column], range: Range): Result = {
        val left = cols(JArrayFixedT(Map(0 -> JNumberT)))
        val right = cols(JArrayFixedT(Map(1 -> JNumberT)))

        val cross = for (l <- left; r <- right) yield (l, r)

        val result: Set[Result] = cross map {
          case (c1: LongColumn, c2: LongColumn) => 
            val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i).toDouble, c2(i).toDouble) }
            reduceDouble(mapped)

          case (c1: LongColumn, c2: DoubleColumn) => 
            val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i).toDouble, c2(i)) }
            reduceDouble(mapped)

          case (c1: LongColumn, c2: NumColumn) => 
            val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i).toDouble, c2(i).toDouble) }
            reduceDouble(mapped)

          case (c1: DoubleColumn, c2: LongColumn) => 
            val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i), c2(i).toDouble) }
            reduceDouble(mapped)

          case (c1: DoubleColumn, c2: DoubleColumn) => 
            val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i), c2(i)) }
            reduceDouble(mapped)

          case (c1: DoubleColumn, c2: NumColumn) => 
            val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i), c2(i).toDouble) }
            reduceDouble(mapped)

          case (c1: NumColumn, c2: LongColumn) => 
            val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i).toDouble, c2(i).toDouble) }
            reduceDouble(mapped)

          case (c1: NumColumn, c2: DoubleColumn) => 
            val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i).toDouble, c2(i)) }
            reduceDouble(mapped)

          case (c1: NumColumn, c2: NumColumn) => 
            val mapped = range filter { r => c1.isDefinedAt(r) && c2.isDefinedAt(r) } map { i => (c1(i).toDouble, c2(i).toDouble) }
            reduceDouble(mapped)

          case _ => None
        }
        
        if (result.isEmpty) {
          None
        } else {
          result.suml(monoid)
        }
      }
    }

    @tailrec
    def gradloop(seq: Seq[(Double, Double)], theta0: Theta, alpha: Double): Theta = {
      val theta = gradient(seq, theta0, alpha)

      val diffs = theta0.zip(theta) map { case (t0, t) => math.abs(t0 - t) }
      val sum = diffs.sum

      if (sum / theta.length < 0.0001) {
        theta
      } else if (cost(seq, theta) > cost(seq, theta0)) {
        if (alpha > Double.MinValue * 2)
          gradloop(seq, theta0, alpha / 2.0)
        else 
          theta0
      } else { 
        gradloop(seq, theta, alpha)
      }
    }

    def extract(res: Result): Table = {
      res map { 
        case seq => {
          val initialTheta: Theta = {
            val theta1s = Seq.fill(1000)(Random.nextGaussian * 10)
            val theta2s = Seq.fill(1000)(Random.nextGaussian * 10)
            val thetas = theta1s zip theta2s

            //todo instead of putting them into map, just keep track of lowest cost
            val costs = thetas.foldLeft(Map.empty[Double, Theta]) {
              case (costs, (t1, t2)) => costs + (cost(seq, Array(t1, t2)) -> Array(t1, t2))
            }

            val minCost = costs.keys min

            costs(minCost)
          }

          val initialAlpha = 1.0

          //println("initial theta0: " + initialTheta(0))
          //println("initial theta1: " + initialTheta(1))

          val finalTheta: Theta = gradloop(seq, initialTheta, initialAlpha)

          //println("final theta1: " + finalTheta(0))
          //println("final theta2: " + finalTheta(1))

          val theta1 = Table.constDecimal(Set(CNum(finalTheta(0))))
          val theta2 = Table.constDecimal(Set(CNum(finalTheta(1))))

          val theta1Spec = trans.WrapObject(Leaf(SourceLeft), "theta1")
          val theta2Spec = trans.WrapObject(Leaf(SourceRight), "theta2")
          val concatSpec = trans.InnerObjectConcat(theta1Spec, theta2Spec)

          val valueTable = theta1.cross(theta2)(trans.WrapObject(concatSpec, paths.Value.name))
          val keyTable = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

          valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
        }
      } getOrElse Table.empty
    }

    def apply(table: Table) = {
      val sampleTable = table.sample(10000, 1)
      sampleTable.flatMap(_.head.reduce(reducer).map(extract))
    }
  }
}
