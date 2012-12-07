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
          //TODO deal with taking and last from empty seq
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
            sum
          }
        }
      }
    
      -result
    }
    
    def gradient(seq: Seq[ColumnValues], theta: Theta, alpha: Double): Theta = { 
      seq.foldLeft(theta) { 
        case (theta, colVal) => {
          //TODO deal with taking and last from empty seq
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
    
    def reduceDouble(seq: Seq[ColumnValues]): Result = {
      if (seq.isEmpty) {
        None
      } else {
        Some(seq)
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

    def extract(res: Result): Table = {
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

          val theta = Table.constArray(Set(CArray[Double](finalTheta)))

          val valueTable = theta.transform(trans.WrapObject(Leaf(Source), paths.Value.name))
          val keyTable = Table.constEmptyArray.transform(trans.WrapObject(Leaf(Source), paths.Key.name))

          valueTable.cross(keyTable)(InnerObjectConcat(Leaf(SourceLeft), Leaf(SourceRight)))
        }
      } getOrElse Table.empty
    }

    def apply(table: Table): M[Table] = {
      val specs: M[Seq[TransSpec1]] = table.schemas map { jtypes => 
        jtypes.toSeq map { jtype => trans.Typed(TransSpec1.Id, jtype) }
      }

      val sampleTables: M[Seq[Table]] = specs flatMap { seq => table.sample(10000, seq) }

      val tableReducer: Table => M[Table] = _.toArray[Double].reduce(reducer).map(extract)
      val reducedTables: M[Seq[Table]] = sampleTables flatMap { _.map(tableReducer).toStream.sequence map( _.toSeq) }

      reducedTables map { _ reduce { _ concat _ } }
    }
  }
}


//the toArray function has in its hands
