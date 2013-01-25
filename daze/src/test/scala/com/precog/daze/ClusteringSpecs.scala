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

import com.precog.common.Path
import com.precog.yggdrasil._
import com.precog.util.IOUtils

import org.specs2.mutable._

import spire.ArrayOps
import spire.implicits._

import blueeyes.json._

import java.io.File

import scala.util.Random._

import scalaz._

trait ClusteringLibSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with ClusteringLib[M]
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

  implicit def arrayOps[@specialized(Double) A](lhs: Array[A]) = new ArrayOps(lhs)

  def kMediansCost(points: Array[Array[Double]], centers: Array[Array[Double]]): Double = {
    points.foldLeft(0.0) { (cost, p) =>
      cost + centers.map({ c => (p - c).norm }).qmin
    }
  }

  case class GeneratedPointSet(points: Array[Array[Double]], centers: Array[Array[Double]])

  def genPoints(n: Int, dimension: Int, k: Int): GeneratedPointSet = {

    def genPoint(x: => Double): Array[Double] = Array.fill(dimension)(x)

    val s = math.pow(2 * k, 1.0 / dimension)
    val centers = (1 to k).map({ _ => genPoint(nextDouble * s) }).toArray
    val points = (1 to n).map({ _ =>
      val c = nextInt(k)
      genPoint(nextGaussian) + centers(c)
    }).toArray

    GeneratedPointSet(points, centers)
  }

  def pointsToJson(points: Array[Array[Double]]): List[JValue] = {
    points.toList map { p =>
      JArray(p.toSeq map (JNum(_)): _*)
    }
  }

  def writePointsToDataset[A](points: Array[Array[Double]])(f: String => A): A = {
    val lines = pointsToJson(points) map { _.renderCompact }
    val tmpFile = File.createTempFile("values", ".json")
    IOUtils.writeSeqToFile(lines, tmpFile).unsafePerformIO
    val pointsString0 = "filesystem" + tmpFile.toString
    val pointsString = pointsString0.take(pointsString0.length - 5)
    val result = f(pointsString)
    tmpFile.delete()
    result
  }

  val ClusterIdPattern = """Cluster\d+""".r

  "k-medians clustering" should {
  /*
    "return something" in {
      val line = Line(0, "")

      val input = dag.Morph2(line, KMediansClustering,
        dag.LoadLocal(line, Const(line, CString("/hom/heightWeightAcrossSlices"))),
        dag.LoadLocal(line, Const(line, CString("/hom/smallnumbers"))))

      val result = testEval(input)

      println("result: " + result)

      result must not beEmpty
    }
    */

    "compute good k-medians clustering" in {
      val dimension = 4
      val k = 5
      val GeneratedPointSet(points, centers) = genPoints(2000, dimension, k)
      val targetCost = kMediansCost(points, centers)
      writePointsToDataset(points) { dataset =>
        val line = Line(0, "")

        val input = dag.Morph2(line, KMediansClustering,
          dag.LoadLocal(line, Const(line, CString(dataset))),
          dag.Const(line, CLong(k)))

        val result = testEval(input)

        result must haveSize(1)

        result must haveAllElementsLike { case (ids, SObject(obj)) if ids.size == 0 => 
          obj.keys mustEqual Set("Model1")
          obj("Model1") must beLike {
            case SObject(clusterMap) =>
              clusterMap must haveSize(k)
              clusterMap.keys must haveAllElementsLike {
                case ClusterIdPattern(_) => ok
              }

              val clusters: Array[Array[Double]] = clusterMap.values.collect({
                case SArray(arr) =>
                  arr must haveSize(dimension)
                  arr.collect({ case SDecimal(n) =>
                    n.toDouble
                  }).toArray
              }).toArray

              val cost = kMediansCost(points, clusters)
              println("cost: " + cost)
              println("targetCost: " + targetCost)
              
              cost must be_<(3 * targetCost)
          }
        }
      }
    }
  }
}

object ClusteringLibSpecs extends ClusteringLibSpecs[test.YId] with test.YIdInstances
