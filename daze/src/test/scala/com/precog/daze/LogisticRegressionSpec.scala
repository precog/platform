package com.precog.daze

import scala.util.Random

import com.precog.yggdrasil._
import com.precog.common.Path
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

  def createSamplePoints(length: Int, noSamples: Int, actualThetas: Array[Double]): Seq[String] = {
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
  
    val seqs = testSeqX zip testSeqY

    seqs map { case (xs, y) => "[" + "[" + xs.mkString(",") + "]," + y + "]" }
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

  def isOk(actual: Double, computed: List[Double]): Boolean = {  
    val (mad, median) = madMedian(computed)
    println("median: " + median)

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

  val testUID = "testUID"

  def testEval(graph: DepGraph): Set[SEvent] = withContext { ctx =>
    consumeEval(testUID, graph, ctx, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  "logistic regression" should {
    /*
    "pass randomly generated test with a single feature" in {
      val line = Line(0, "")

      val actualThetas = makeThetas(2)

      println("t1: " + actualThetas(0))
      println("t2: " + actualThetas(1))

      var thetas: List[List[Double]] = List.empty[List[Double]]
      var i = 0

      //runs the logistic regression function on 50 sets of data generated from the same distribution
      while (i < 50) {
        val points = createSamplePoints(2, 1000, actualThetas)

        val tmpFile = File.createTempFile("values", ".json")
        IOUtils.writeSeqToFile(points, tmpFile).unsafePerformIO

        val pointsString0 = "filesystem" + tmpFile.toString
        val pointsString = pointsString0.take(pointsString0.length - 5)
        
        val input = dag.Morph2(line, LogisticRegression,
          dag.Join(line, DerefArray, CrossLeftSort,
            dag.LoadLocal(line, Root(line, CString(pointsString))),
            dag.Root(line, CLong(0))),
          dag.Join(line, DerefArray, CrossLeftSort,
            dag.LoadLocal(line, Root(line, CString(pointsString))),
            dag.Root(line, CLong(1))))

        val result = testEval(input)
        tmpFile.delete()

        val theta = result collect {
          case (ids, SArray(elems)) if ids.length == 0 => {
            val SDecimal(theta1) = elems(0)
            val SDecimal(theta2) = elems(1) match { case SArray(elems2) => elems2(0) }
            List(theta1.toDouble, theta2.toDouble)
          }
        }

        thetas = thetas ++ theta
        i += 1
      }

      val theta1s = thetas map { l => l(0) }
      val theta2s = thetas map { l => l(1) }
       
      isOk(actualThetas(0), theta1s) mustEqual(true)
      isOk(actualThetas(1), theta2s) mustEqual(true)
    }*/

    "pass randomly generated test with three features" in {
      val line = Line(0, "")

      val actualThetas = makeThetas(4)

      println("t0n: " + actualThetas(0))
      println("t1n: " + actualThetas(1))
      println("t2n: " + actualThetas(2))
      println("t3n: " + actualThetas(3))

      var thetas: List[List[Double]] = List.empty[List[Double]]
      var i = 0

      //runs the logistic regression function on 50 sets of data generated from the same distribution
      while (i < 50) {
        val points = createSamplePoints(4, 1000, actualThetas)

        val tmpFile = File.createTempFile("values", ".json")
        IOUtils.writeSeqToFile(points, tmpFile).unsafePerformIO

        val pointsString0 = "filesystem" + tmpFile.toString
        val pointsString = pointsString0.take(pointsString0.length - 5)
        
        val input = dag.Morph2(line, LogisticRegression,
          dag.Join(line, DerefArray, CrossLeftSort,
            dag.LoadLocal(line, Root(line, CString(pointsString))),
            dag.Root(line, CLong(0))),
          dag.Join(line, DerefArray, CrossLeftSort,
            dag.LoadLocal(line, Root(line, CString(pointsString))),
            dag.Root(line, CLong(1))))

        val result = testEval(input)
        tmpFile.delete()

        val theta = result collect {
          case (ids, SArray(elems)) if ids.length == 0 => {
            println("elems: " + elems)
            val SDecimal(theta0) = elems(0)
            val SDecimal(theta1) = elems(1) match { case SArray(elems2) => elems2(0) }
            val SDecimal(theta2) = elems(1) match { case SArray(elems2) => elems2(1) }
            val SDecimal(theta3) = elems(1) match { case SArray(elems2) => elems2(2) }
            List(theta0.toDouble, theta1.toDouble, theta2.toDouble, theta3.toDouble)
          }
        }

        thetas = thetas ++ theta
        i += 1
      }

      val theta0s = thetas map { l => l(0) }
      val theta1s = thetas map { l => l(1) }
      val theta2s = thetas map { l => l(2) }
      val theta3s = thetas map { l => l(3) }
       
      isOk(actualThetas(0), theta0s) mustEqual(true)
      isOk(actualThetas(1), theta1s) mustEqual(true)
      isOk(actualThetas(2), theta2s) mustEqual(true)
      isOk(actualThetas(3), theta3s) mustEqual(true)
    }
  }
}

object LogisticRegressionSpec extends LogisticRegressionSpec[test.YId] with test.YIdInstances
