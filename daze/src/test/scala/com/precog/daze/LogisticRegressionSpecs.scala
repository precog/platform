package com.precog.daze

import scala.util.Random
import scala.collection.mutable

import com.precog.common._
import com.precog.yggdrasil._
import com.precog.yggdrasil.util.CPathUtils._
import com.precog.common.Path
import com.precog.common.json._
import com.precog.util.IOUtils

import org.specs2.mutable._

import java.io.File

import scalaz._

trait LogisticRegressionTestSupport[M[+_]] extends StdLibEvaluatorStack[M]
    with RegressionTestSupport[M] {
  import library._
  import instructions._
  import library._
  import dag._

  def predictionInput(morph: Morphism2, modelData: String, model: String) = {
    val line = Line(0, 0, "")
    dag.Morph2(morph,
      dag.LoadLocal(Const(CString(modelData))(line))(line),
      dag.LoadLocal(Const(CString(model))(line))(line)
    )(line)
  }

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

trait LogisticRegressionSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M] 
    with LogisticRegressionTestSupport[M]
    with LongIdMemoryDatasetConsumer[M]{ self =>

  import dag._
  import instructions._
  import library._

  val testAPIKey = "testAPIKey"

  def testEval(graph: DepGraph): Set[SEvent] = {
    consumeEval(testAPIKey, graph, Path.Root) match {
      case Success(results) => results
      case Failure(error) => throw error
    }
  }

  def makeDAG(points: String) = {
    val line = Line(1, 1, "")

    dag.Morph2(LogisticRegression,
      dag.Join(DerefArray, CrossLeftSort,
        dag.LoadLocal(Const(CString(points))(line))(line),
        dag.Const(CLong(1))(line))(line),
      dag.Join(DerefArray, CrossLeftSort,
        dag.LoadLocal(Const(CString(points))(line))(line),
        dag.Const(CLong(0))(line))(line))(line)
  }

  def returnCoeff(obj: Map[String, SValue]) = {
    val coeff = "coefficient"

    obj.keys mustEqual Set(coeff)  
    obj(coeff)
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
      
      val input = makeDAG(pointsString)

      val result = testEval(input)
      tmpFile.delete()

      val theta = result collect {
        case (ids, SObject(elems)) if ids.length == 0 =>
		      elems.keys mustEqual Set("Model1")
		      val SArray(arr) = elems("Model1")

          val SDecimal(theta1) = (arr(0): @unchecked) match { case SArray(elems2) =>
            (elems2(0): @unchecked) match { case SObject(obj) =>
              returnCoeff(obj)
            }
          }

          val SDecimal(theta0) = (arr(1): @unchecked) match { case SObject(obj) =>
            returnCoeff(obj)
          }
            
          List(theta0.toDouble, theta1.toDouble)
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
      
      val input = makeDAG(pointsString) 

      val result = testEval(input)
      tmpFile.delete()

      val theta = result collect {
        case (ids, SObject(elems)) if ids.length == 0 => {
          elems.keys mustEqual Set("Model1")
 		  val SArray(arr) = elems("Model1")

          val SDecimal(theta1) = (arr(0): @unchecked) match { case SObject(map) =>
            (map("bar"): @unchecked) match { case SObject(obj) =>
              returnCoeff(obj)
            }
          }
          val SDecimal(theta2) = (arr(0): @unchecked) match { case SObject(map) =>
            (map("baz"): @unchecked) match { case SObject(obj) =>
              returnCoeff(obj)
            }
          }
          val SDecimal(theta3) = (arr(0): @unchecked) match { case SObject(map) =>
            (map("foo"): @ unchecked) match { case SObject(obj) =>
              returnCoeff(obj)
            }
          }
          val SDecimal(theta0) = (arr(1): @unchecked) match { case SObject(obj) =>
            returnCoeff(obj)
          }

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
      
      val input = makeDAG(pointsString)

      val result = testEval(input)
      tmpFile.delete()

      result must haveSize(1)

      def theta(model: String) = result collect {
        case (ids, SObject(elems)) if ids.length == 0 => {
          elems.keys mustEqual Set("Model1", "Model2", "Model3")
          val SArray(arr) = elems(model)

          val SDecimal(theta1) = (arr(0): @unchecked) match { case SObject(map) => 
            (map("bar"): @unchecked) match { case SObject(map) => 
              (map("baz"): @unchecked) match { case SArray(elems) =>
                (elems(0): @unchecked) match { case SObject(obj) =>
                  returnCoeff(obj)
                }
              }
            } 
          }

          val SDecimal(theta2) = (arr(0): @unchecked) match { case SObject(map) =>
            (map("foo"): @unchecked) match { case SObject(obj) =>
              returnCoeff(obj)
            }
          }

          val SDecimal(theta0) = (arr(1): @unchecked) match { case SObject(map) =>
            returnCoeff(map)
          }

          List(theta0.toDouble, theta1.toDouble, theta2.toDouble)
        }
      }

      thetasSchema1 = thetasSchema1 ++ theta("Model1")
      thetasSchema2 = thetasSchema2 ++ theta("Model2")
      thetasSchema3 = thetasSchema3 ++ theta("Model3")

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

  "logistic prediction" should {
    "predict simple case" in {
      val line = Line(0, 0, "")

      val input = predictionInput(LogisticPrediction, "/hom/model1data", "/hom/model1")

      val result0 = testEval(input)

      result0 must haveSize(19)

      val result = result0 collect { case (ids, value) if ids.size == 2 => value }

      result mustEqual Set(
        (SObject(Map("Model2" -> SDecimal(3.487261531994447E-19), "Model1" -> SDecimal(8.644057113036095E-22)))),
        (SObject(Map("Model2" -> SDecimal(1.5628821893349888E-18)))),
        (SObject(Map("Model2" -> SDecimal(0.0003353501304664781), "Model1" -> SDecimal(0.000006144174602214718)))),
        (SObject(Map("Model2" -> SDecimal(4.1399375473943306E-8), "Model1" -> SDecimal(0.0013585199504289591)))),
        (SObject(Map("Model2" -> SDecimal(5.109089028037222E-12), "Model1" -> SDecimal(3.7751345441365816E-11)))), 
        (SObject(Map("Model2" -> SDecimal(1.0261879630648827E-10), "Model1" -> SDecimal(6.305116760146985E-16)))),
        (SObject(Map("Model2" -> SDecimal(2.543665647376276E-13), "Model1" -> SDecimal(1.1548224173015786E-17)))), 
        (SObject(Map("Model2" -> SDecimal(0.11920292202211755), "Model1" -> SDecimal(0.5)))), 
        (SObject(Map("Model2" -> SDecimal(0.9999998874648379), "Model1" -> SDecimal(0.9999999847700205)))), 
        (SObject(Map("Model3" -> SDecimal(0.00007484622751061124)))),
        (SObject(Map("Model3" -> SDecimal(0.0009110511944006454)))),
        (SObject(Map("Model3" -> SDecimal(0.999983298578152)))),
        (SObject(Map("Model3" -> SDecimal(2.646573631904765E-9)))),
        (SObject(Map("Model3" -> SDecimal(0.6224593312018546)))),
        (SObject(Map("Model3" -> SDecimal(4.1399375473943306E-8)))),
        (SObject(Map("Model3" -> SDecimal(5.043474082014517E-7)))),
        (SObject(Map("Model3" -> SDecimal(0.6224593312018546)))),
        (SObject(Map("Model3" -> SDecimal(2.289734845593124E-11)))),
        (SObject(Map("Model3" -> SDecimal(0.6224593312018546)))))
    }

    "predict case with repeated model names and arrays" in {
      val line = Line(0, 0, "")

      val input = predictionInput(LogisticPrediction, "/hom/model2data", "/hom/model2")

      val result0 = testEval(input)

      result0 must haveSize(14)

      val result = result0 collect { case (ids, value) if ids.size == 2 => value }

      result mustEqual Set(
        (SObject(Map("Model1" -> SDecimal(0.0003353501304664781), "Model3" -> SDecimal(1.522997951276035E-8)))),
        (SObject(Map("Model1" -> SDecimal(4.1399375473943306E-8)))),
        (SObject(Map("Model1" -> SDecimal(1.0261879630648827E-10)))),
        (SObject(Map("Model1" -> SDecimal(0.11920292202211755)))),
        (SObject(Map("Model2" -> SDecimal(8.315280276641321E-7), "Model1" -> SDecimal(0.00012339457598623172)))),
        (SObject(Map("Model1" -> SDecimal(1.522997951276035E-8)))), 
        (SObject(Map("Model1" -> SDecimal(3.7751345441365816E-11)))), 
        (SObject(Map("Model1" -> SDecimal(0.04742587317756678)))),
        (SObject(Map("Model3" -> SDecimal(0.5)))),
        (SObject(Map("Model3" -> SDecimal(0.000746028833836697)))), 
        (SObject(Map("Model3" -> SDecimal(0.9939401985084158)))), 
        (SObject(Map("Model2" -> SDecimal(2.319522830243569E-16)))),
        (SObject(Map("Model3" -> SDecimal(0.9820137900379085)))), 
        (SObject(Map("Model2" -> SDecimal(3.625140919143559E-34)))))
    }
  }
}

object LogisticRegressionSpecs extends LogisticRegressionSpecs[test.YId] with test.YIdInstances
