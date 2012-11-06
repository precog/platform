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

    type Result = Option[Theta => Double]
    type Theta = Array[Double]

    //TODO will need this monoid when we also return the gradient function from the reduction
    //implicit def monoidCompose = new Monoid[Theta => Theta] {
    //  def zero = identity _
    //  def append(t1: Theta => Theta, t2: => Theta => Theta) = {
    //    t1 compose t2
    //  }
    //}

    implicit def monoid = implicitly[Monoid[Result]]

    def reduceDouble(seq: Seq[(Double, Double)]): Result = {
      if (seq.isEmpty) {
        None
      } else {
        def cost(theta: Theta): Double = {
          val result = seq.foldLeft(0D) {
            case (sum, (x, y)) => {
              if (y == 1) {
                sum + log(sigmoid(theta(0) + theta(1) * x))
              }
              else if (y == 0) {
                sum + log(1 - sigmoid(theta(0) + theta(1) * x))
              }
              else
                sum //in quirrel-like fashion, this ignores dependent variable values not equal to 0 or 1
            }
          }

          -result
        }

        Some(cost _)
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

    //TODO generalize this using the calculated partials!!!!
    //that way we'll be taking a step *exactly* in the direction of the gradient
    //and we'll take a step every time
    def shiftTheta(theta: Theta, cost: Theta => Double, alpha: Double): Theta = {
      val cost_center = cost(theta)

      val cost_up = cost(Array(theta(0) + alpha, theta(1)))
      val cost_down = cost(Array(theta(0) - alpha, theta(1)))
      val cost_right = cost(Array(theta(0), theta(1) + alpha))
      val cost_left = cost(Array(theta(0), theta(1) - alpha))

      val t0 = {
        if (cost_up < cost_center) 
          theta(0) + alpha
        else if (cost_down < cost_center)
          theta(0) - alpha
        else
          theta(0)  
      }

      val t1 = {
        if (cost_right < cost_center)
          theta(1) + alpha
        else if (cost_left < cost_center)
          theta(1) - alpha
        else
          theta(1)  
      }

      if (cost(Array(t0, t1)) < cost_center)
        Array(t0, t1)
      else
        theta
    }

    def extract(res: Result): Table = {
      res map { 
        case cost => {
          //TODO when the shiftTheta uses stochastic gradient descent, 
          //this function will no longer need the counter
          //because we'll have a resonalbe way to determine convergence
          def loop(theta: Theta, alpha: Double, minAlpha: Double, counter: Int): Theta = {
            if (counter > 500) {
              theta
            } else { 
              val result = shiftTheta(theta, cost, alpha)

              if (alpha < minAlpha) {
                result
              } else {
                if (theta(0) == result(0) && theta(1) == result(1))
                  loop(result, alpha / 2, minAlpha, counter + 1)
                else
                  loop(result, alpha, minAlpha, counter + 1)
              }
            }
          }

          //TODO big question: what can we learn from the data that will help us better determine these starting values?
          val finalTheta: Theta = loop(Array(0, 0), 5, 0.000001, 0)

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

    def apply(table: Table) = table.reduce(reducer) map extract
  }
}
