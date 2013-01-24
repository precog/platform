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

import com.precog.yggdrasil._

import com.precog.common.Path
import com.precog.common.json._
import com.precog.util.IOUtils

import org.specs2.mutable._

import java.io.File

import scalaz._

trait LinearRegressionTestSupport[M[+_]] extends LinearRegressionLib[M] with RegressionTestSupport[M] {
  def createLinearSamplePoints(length: Int, noSamples: Int, actualThetas: Array[Double]): Seq[(Array[Double], Double)] = {
    val testSeqX = {
      def createXs: Array[Double] = {
        Seq.fill(length - 1)(Random.nextDouble) map {
          x => x * 2.0 - 1.0
        } toArray
      }

      Seq.fill(noSamples)(createXs)
    }

    val deciders = Seq.fill(noSamples)(Random.nextDouble)

    val testSeqY = {
      testSeqX map { 
        case xs => {
          val yvalue = dotProduct(actualThetas, 1.0 +: xs)
          yvalue + Random.nextGaussian
        }
      }
    }
  
    testSeqX zip testSeqY
  }
}

trait LinearRegressionSpec[M[+_]] extends Specification 
    with EvaluatorTestSupport[M]
    with LinearRegressionTestSupport[M]
    with LongIdMemoryDatasetConsumer[M] { self =>

  import dag._
  import instructions._

  def testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  def testTrivial = {
    val line = Line(1, 1, "")

    val num = 2
    val loops = 50

    val actualThetas = makeThetas(num)

    var thetas = List.empty[List[Double]]
    var i = 0

    //runs the linear regression function on 50 sets of data generated from the same distribution
    while (i < loops) {
      val cpaths = Seq(
        CPath(CPathIndex(0), CPathIndex(0)),
        CPath(CPathIndex(1))) sorted

      val samples = createLinearSamplePoints(num, 100, actualThetas)
      val points = jvalues(samples, cpaths) map { _.toString }

      val tmpFile = File.createTempFile("values", ".json")
      IOUtils.writeSeqToFile(points, tmpFile).unsafePerformIO

      val pointsString0 = "filesystem" + tmpFile.toString
      val pointsString = pointsString0.take(pointsString0.length - 5)
      
      val input = dag.Morph2(MultiLinearRegression,
        dag.Join(DerefArray, CrossLeftSort,
          dag.LoadLocal(Const(CString(pointsString))(line))(line),
          dag.Const(CLong(0))(line))(line),
        dag.Join(DerefArray, CrossLeftSort,
          dag.LoadLocal(Const(CString(pointsString))(line))(line),
          dag.Const(CLong(1))(line))(line))(line)

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

  def testThreeFeatures = {
    val line = Line(1, 1, "")

    val num = 4
    val loops = 50

    val actualThetas = makeThetas(num)

    var thetas: List[List[Double]] = List.empty[List[Double]]
    var i = 0

    //runs the linear regression function on 50 sets of data generated from the same distribution
    while (i < loops) {
      val cpaths = Seq(
        CPath(CPathIndex(0), CPathField("foo")),
        CPath(CPathIndex(0), CPathField("bar")),
        CPath(CPathIndex(0), CPathField("baz")),
        CPath(CPathIndex(1))) sorted

      val samples = createLinearSamplePoints(num, 100, actualThetas)
      val points = jvalues(samples, cpaths) map { _.toString }

      val tmpFile = File.createTempFile("values", ".json")
      IOUtils.writeSeqToFile(points, tmpFile).unsafePerformIO

      val pointsString0 = "filesystem" + tmpFile.toString
      val pointsString = pointsString0.take(pointsString0.length - 5)
      
      val input = dag.Morph2(MultiLinearRegression,
        dag.Join(DerefArray, CrossLeftSort,
          dag.LoadLocal(Const(CString(pointsString))(line))(line),
          dag.Const(CLong(0))(line))(line),
        dag.Join(DerefArray, CrossLeftSort,
          dag.LoadLocal(Const(CString(pointsString))(line))(line),
          dag.Const(CLong(1))(line))(line))(line)

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

  def testThreeSchemata = {
    val line = Line(1, 1, "")

    val num = 3
    val loops = 50

    val actualThetas = makeThetas(num)

    var thetasSchema1 = List.empty[List[Double]]
    var thetasSchema2 = List.empty[List[Double]]
    var thetasSchema3 = List.empty[List[Double]]

    var i = 0

    //runs the linear regression function on 50 sets of data generated from the same distribution
    while (i < loops) {
      val cpaths = Seq(
        CPath(CPathIndex(0), CPathField("ack"), CPathIndex(0)),
        CPath(CPathIndex(0), CPathField("bak"), CPathField("bazoo")),
        CPath(CPathIndex(0), CPathField("bar"), CPathField("baz"), CPathIndex(0)),
        CPath(CPathIndex(0), CPathField("foo")),
        CPath(CPathIndex(1))) sorted

      val samples = {
        val samples0 = createLinearSamplePoints(num, 100, actualThetas)
        samples0 map { case (xs, y) => (Random.nextGaussian +: Random.nextGaussian +: xs, y) }
      }
      val points = jvalues(samples, cpaths, num) map { _.toString }

      val suffix = ".json"
      val tmpFile = File.createTempFile("values", suffix)
      IOUtils.writeSeqToFile(points, tmpFile).unsafePerformIO

      val pointsString0 = "filesystem" + tmpFile.toString
      val pointsString = pointsString0.take(pointsString0.length - suffix.length)
      
      val input = dag.Morph2(MultiLinearRegression,
        dag.Join(DerefArray, CrossLeftSort,
          dag.LoadLocal(Const(CString(pointsString))(line))(line),
          dag.Const(CLong(0))(line))(line),
        dag.Join(DerefArray, CrossLeftSort,
          dag.LoadLocal(Const(CString(pointsString))(line))(line),
          dag.Const(CLong(1))(line))(line))(line)

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
    val expected = Array.fill(num)(true)

    result(0) mustEqual expected
    result(1) mustEqual expected
    result(2) mustEqual expected
  }

  "linear regression" should {
    "pass randomly generated test with a single feature" in (testTrivial or testTrivial)
    "pass randomly generated test with three features inside an object" in (testThreeFeatures or testThreeFeatures)
    "pass randomly generated test with three distinct schemata" in (testThreeSchemata or testThreeSchemata)
  }
}

object LinearRegressionSpec extends LinearRegressionSpec[test.YId] with test.YIdInstances
