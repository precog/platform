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
package com.precog.daze

import scala.util.Random
import scala.collection.mutable

import blueeyes.json._

import com.precog.yggdrasil._
import com.precog.yggdrasil.util.CPathUtils._
import com.precog.common.Path
import com.precog.common.json._
import com.precog.util.IOUtils

import org.specs2.mutable._

import java.io.File

import scalaz._

trait RegressionTestSupport[M[+_]] extends RegressionLib[M] {
  import LogisticRegression.dotProduct

  def arraySum(xs: Array[Double], ys: Array[Double]): Array[Double] = {
    assert(xs.length == ys.length)
    var i = 0
    var result = new Array[Double](xs.length)
    while (i < xs.length) {
      result(i) = xs(i) + ys(i)
      i += 1
    }
    result
  }    

  def sigmoid(z: Double): Double = 1 / (1 + math.exp(z))

  def makeT: Double = {
    val theta = Random.nextGaussian * 10
    if (theta == 0) makeT
    else theta
  }

  def makeThetas(length: Int): Array[Double] = {
    Seq.fill(length)(makeT).toArray
  }

  def createSamplePoints(length: Int, noSamples: Int, actualThetas: Array[Double]): Seq[(Array[Double], Int)] = {
    val direction: Array[Double] = {
      var result = new Array[Double](actualThetas.length - 1)
      result(0) = -actualThetas(0) / actualThetas(1)
      result
    }

    val testSeqX = {
      def createXs: Array[Double] = {
        val seq = Seq.fill(length - 1)(Random.nextDouble) map {
          x => x * 2.0 - 1.0
        } toArray

        arraySum(seq, direction)
      }

      Seq.fill(noSamples)(createXs)
    }

    val deciders = Seq.fill(noSamples)(Random.nextDouble)

    val testSeqY = {
      (testSeqX zip deciders) map { 
        case (xs, p) => {
          val product: Double = dotProduct(actualThetas, 1.0 +: xs)
          if (sigmoid(product) > p) 1
          else 0
        }
      }
    }
  
    testSeqX zip testSeqY
  }

  def jvalues(samples: Seq[(Array[Double], Int)], cpaths: Seq[CPath], mod: Int = 1): Seq[JValue] = samples.zipWithIndex map { case ((xs, y), idx) => 
    val cvalues = xs.map { x => CDouble(x).asInstanceOf[CValue] } :+ CLong(y).asInstanceOf[CValue] 
    val withCPath = {
      if (idx % mod == 0) cpaths zip cvalues.toSeq
      else if (idx % mod == 1) cpaths.tail zip cvalues.tail.toSeq
      else cpaths.tail.tail zip cvalues.tail.tail.toSeq
    }
    val withJPath = withCPath map { case (cpath, cvalue) => cPathToJPaths(cpath, cvalue) head }  // `head` is only okay if we don't have any homogeneous arrays
    val withJValue = withJPath map { case (jpath, cvalue) => (jpath, cvalue.toJValue) }
    withJValue.foldLeft(JArray(Nil).asInstanceOf[JValue]) { case (target, (jpath, jvalue)) => target.unsafeInsert(jpath, jvalue) }
  } 

  def stdDevMean(values: List[Double]): (Double, Double) = {
    val count = values.size
    val sum = values.sum
    val sumsq = values map { x => math.pow(x, 2) } sum

    val stdDev = math.sqrt(count * sumsq - sum * sum) / count
    val mean = sum / count

    (stdDev, mean)
  }

  //more robust way to deal with outliers than stdDev
  //the `constant` is the conversion constant to the units of standard deviation
  def madMedian(values: List[Double]): (Double, Double) = {
    val constant = 0.6745

    val sorted = values.sorted
    val length = sorted.length
    val median = sorted(length / 2)

    val diffs = values map { v => math.abs(v - median) }
    val sortedDiffs = diffs.sorted
    val mad = sortedDiffs(length / 2) / constant

    (mad, median) 
  }

  def combineResults(num: Int, thetas: List[List[Double]]) = {
    thetas.foldLeft(mutable.Seq.fill(num)(List.empty[Double])) { case (acc, li) => 
      var i = 0
      while (i < li.length) {
        acc(i) = acc(i) :+ li(i)
        i += 1
      }
      acc
    }
  }

  def isOk(actual: Double, computed: List[Double]): Boolean = {  
    val (mad, median) = madMedian(computed)

    val diff = math.abs(median - actual)
    val (dev, mean) = stdDevMean(computed)

    diff < mad * 3D
  }
}

trait LogisticRegressionSpec[M[+_]] extends Specification
    with EvaluatorTestSupport[M] 
    with RegressionTestSupport[M]
    with MemoryDatasetConsumer[M]{ self =>

  import dag._
  import instructions._

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph): Set[SEvent] = withContext { ctx =>
    consumeEval(testAPIKey, graph, ctx, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "logistic regression" should {
    "pass randomly generated test with a single feature" in {
      val line = Line(0, "")

      val num = 2

      val actualThetas = makeThetas(num)

      var thetas: List[List[Double]] = List.empty[List[Double]]
      var i = 0

      //runs the logistic regression function on 50 sets of data generated from the same distribution
      while (i < 50) {
        val cpaths = Seq(
          CPath(CPathIndex(0), CPathIndex(0)),
          CPath(CPathIndex(1))) sorted

        val samples = createSamplePoints(num, 1000, actualThetas)
        val points = jvalues(samples, cpaths) map { _.toString }

        val tmpFile = File.createTempFile("values", ".json")
        IOUtils.writeSeqToFile(points, tmpFile).unsafePerformIO

        val pointsString0 = "filesystem" + tmpFile.toString
        val pointsString = pointsString0.take(pointsString0.length - 5)
        
        val input = dag.Morph2(line, LogisticRegression,
          dag.Join(line, DerefArray, CrossLeftSort,
            dag.LoadLocal(line, Const(line, CString(pointsString))),
            dag.Const(line, CLong(0))),
          dag.Join(line, DerefArray, CrossLeftSort,
            dag.LoadLocal(line, Const(line, CString(pointsString))),
            dag.Const(line, CLong(1))))

        val result = testEval(input)
        tmpFile.delete()

        val theta = result collect {
          case (ids, SArray(elems)) if ids.length == 0 => {
            val SDecimal(theta1) = elems(0) match { case SArray(elems2) => elems2(0) }
            val SDecimal(theta0) = elems(1)
            List(theta0.toDouble, theta1.toDouble)
          }
        }

        thetas = thetas ++ theta
        i += 1
      }

      val allThetas = actualThetas zip combineResults(num, thetas)

      val ok = allThetas map { case (t, ts) => isOk(t, ts) }

      ok mustEqual Array.fill(num)(true)
    }

    "pass randomly generated test with three features inside an object" in {
      val line = Line(0, "")

      val num = 4

      val actualThetas = makeThetas(num)

      var thetas: List[List[Double]] = List.empty[List[Double]]
      var i = 0

      //runs the logistic regression function on 50 sets of data generated from the same distribution
      while (i < 50) {
        val cpaths = Seq(
          CPath(CPathIndex(0), CPathField("foo")),
          CPath(CPathIndex(0), CPathField("bar")),
          CPath(CPathIndex(0), CPathField("baz")),
          CPath(CPathIndex(1))) sorted

        val samples = createSamplePoints(num, 1000, actualThetas)
        val points = jvalues(samples, cpaths) map { _.toString }

        val tmpFile = File.createTempFile("values", ".json")
        IOUtils.writeSeqToFile(points, tmpFile).unsafePerformIO

        val pointsString0 = "filesystem" + tmpFile.toString
        val pointsString = pointsString0.take(pointsString0.length - 5)
        
        val input = dag.Morph2(line, LogisticRegression,
          dag.Join(line, DerefArray, CrossLeftSort,
            dag.LoadLocal(line, Const(line, CString(pointsString))),
            dag.Const(line, CLong(0))),
          dag.Join(line, DerefArray, CrossLeftSort,
            dag.LoadLocal(line, Const(line, CString(pointsString))),
            dag.Const(line, CLong(1))))

        val result = testEval(input)
        tmpFile.delete()

        val theta = result collect {
          case (ids, SArray(elems)) if ids.length == 0 => {
            val SDecimal(theta1) = elems(0) match { case SObject(map) => map("bar") }
            val SDecimal(theta2) = elems(0) match { case SObject(map) => map("baz") }
            val SDecimal(theta3) = elems(0) match { case SObject(map) => map("foo") }
            val SDecimal(theta0) = elems(1) 
            List(theta0.toDouble, theta1.toDouble, theta2.toDouble, theta3.toDouble)
          }
        }

        thetas = thetas ++ theta
        i += 1
      }

      val allThetas = actualThetas zip combineResults(num, thetas)

      val ok = allThetas map { case (t, ts) => isOk(t, ts) }

      ok mustEqual Array.fill(num)(true)
    }

    "pass randomly generated test with three distinct schemata" in {
      val line = Line(0, "")

      val num = 3

      val actualThetas = makeThetas(num)

      var thetasSchema1 = List.empty[List[Double]]
      var thetasSchema2 = List.empty[List[Double]]
      var thetasSchema3 = List.empty[List[Double]]

      var i = 0

      //runs the logistic regression function on 50 sets of data generated from the same distribution
      while (i < 50) {
        val cpaths = Seq(
          CPath(CPathIndex(0), CPathField("ack"), CPathIndex(0)),
          CPath(CPathIndex(0), CPathField("bak"), CPathField("bazoo")),
          CPath(CPathIndex(0), CPathField("bar"), CPathField("baz"), CPathIndex(0)),
          CPath(CPathIndex(0), CPathField("foo")),
          CPath(CPathIndex(1))) sorted

        val samples = {
          val samples0 = createSamplePoints(num, 1000, actualThetas)
          samples0 map { case (xs, y) => (Random.nextGaussian +: Random.nextGaussian +: xs, y) }
        }
        val points = jvalues(samples, cpaths, num) map { _.toString }

        val suffix = ".json"
        val tmpFile = File.createTempFile("values", suffix)
        IOUtils.writeSeqToFile(points, tmpFile).unsafePerformIO

        val pointsString0 = "filesystem" + tmpFile.toString
        val pointsString = pointsString0.take(pointsString0.length - suffix.length)
        
        val input = dag.Morph2(line, LogisticRegression,
          dag.Join(line, DerefArray, CrossLeftSort,
            dag.LoadLocal(line, Const(line, CString(pointsString))),
            dag.Const(line, CLong(0))),
          dag.Join(line, DerefArray, CrossLeftSort,
            dag.LoadLocal(line, Const(line, CString(pointsString))),
            dag.Const(line, CLong(1))))

        val result = testEval(input)
        tmpFile.delete()

        result must haveSize(3)

        def theta(numOfFields: Int) = result collect {
          case (ids, SArray(elems)) if ids.length == 0 && elems(0).asInstanceOf[SObject].fields.size == numOfFields => {
            val SDecimal(theta1) = elems(0) match { case SObject(map) => 
              map("bar") match { case SObject(map) => 
                map("baz") match { case SArray(elems) =>
                  elems(0)
                }
              } 
            }
            val SDecimal(theta2) = elems(0) match { case SObject(map) => map("foo") }
            val SDecimal(theta0) = elems(1) 
            List(theta0.toDouble, theta1.toDouble, theta2.toDouble)
          }
        }

        thetasSchema1 = thetasSchema1 ++ theta(2)
        thetasSchema2 = thetasSchema2 ++ theta(3)
        thetasSchema3 = thetasSchema3 ++ theta(4)

        i += 1
      }

      def getBooleans(thetas: List[List[Double]]): Array[Boolean] = {
        val zipped = actualThetas zip combineResults(num, thetas)
        zipped map { case (t, ts) => isOk(t, ts) }
      }

      val allThetas = List(thetasSchema1, thetasSchema2, thetasSchema3)

      val result = allThetas map { getBooleans }

      result foreach { _ mustEqual Array.fill(num)(true) }
    }
  }
}

object LogisticRegressionSpec extends LogisticRegressionSpec[test.YId] with test.YIdInstances
