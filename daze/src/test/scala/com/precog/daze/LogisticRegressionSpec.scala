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

trait LogisticRegressionTestSupport[M[+_]] extends LogisticRegressionLib[M] with RegressionTestSupport[M] {
  def sigmoid(z: Double): Double = 1 / (1 + math.exp(z))

  def createLogisticSamplePoints(length: Int, noSamples: Int, actualThetas: Array[Double]): Seq[(Array[Double], Double)] = {
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
          if (sigmoid(product) > p) 1.0
          else 0.0
        }
      }
    }
  
    testSeqX zip testSeqY
  }
}

trait LogisticRegressionSpec[M[+_]] extends Specification
    with EvaluatorTestSupport[M] 
    with LogisticRegressionTestSupport[M]
    with LongIdMemoryDatasetConsumer[M]{ self =>

  import dag._
  import instructions._

  val testAPIKey = "testAPIKey"

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

    //runs the logistic regression function on 50 sets of data generated from the same distribution
    while (i < loops) {
      val cpaths = Seq(
        CPath(CPathIndex(0), CPathIndex(0)),
        CPath(CPathIndex(1))) sorted

      val samples = createLogisticSamplePoints(num, 100, actualThetas)
      val points = jvalues(samples, cpaths) map { _.renderCompact }

      val tmpFile = File.createTempFile("values", ".json")
      IOUtils.writeSeqToFile(points, tmpFile).unsafePerformIO

      val pointsString0 = "filesystem" + tmpFile.toString
      val pointsString = pointsString0.take(pointsString0.length - 5)
      
      val input = dag.Morph2(LogisticRegression,
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

    var thetas = List.empty[List[Double]]
    var i = 0

    //runs the logistic regression function on 50 sets of data generated from the same distribution
    while (i < 50) {
      val cpaths = Seq(
        CPath(CPathIndex(0), CPathField("foo")),
        CPath(CPathIndex(0), CPathField("bar")),
        CPath(CPathIndex(0), CPathField("baz")),
        CPath(CPathIndex(1))) sorted

      val samples = createLogisticSamplePoints(num, 100, actualThetas)
      val points = jvalues(samples, cpaths) map { _.renderCompact }

      val tmpFile = File.createTempFile("values", ".json")
      IOUtils.writeSeqToFile(points, tmpFile).unsafePerformIO

      val pointsString0 = "filesystem" + tmpFile.toString
      val pointsString = pointsString0.take(pointsString0.length - 5)
      
      val input = dag.Morph2(LogisticRegression,
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

  def testThreeSchema = {
    val line = Line(1, 1, "")

    val num = 3
    val loops = 50

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
        val samples0 = createLogisticSamplePoints(num, 100, actualThetas)
        samples0 map { case (xs, y) => (Random.nextGaussian +: Random.nextGaussian +: xs, y) }
      }
      val points = jvalues(samples, cpaths, num) map { _.renderCompact }

      val suffix = ".json"
      val tmpFile = File.createTempFile("values", suffix)
      IOUtils.writeSeqToFile(points, tmpFile).unsafePerformIO

      val pointsString0 = "filesystem" + tmpFile.toString
      val pointsString = pointsString0.take(pointsString0.length - suffix.length)
      
      val input = dag.Morph2(LogisticRegression,
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

  "logistic regression" should {
    "pass randomly generated test with a single feature" in (testTrivial or testTrivial)
    "pass randomly generated test with three features inside an object" in (testThreeFeatures or testThreeFeatures)
    "pass randomly generated test with three distinct schemata" in (testThreeSchema or testThreeSchema)
  }
}

object LogisticRegressionSpec extends LogisticRegressionSpec[test.YId] with test.YIdInstances
