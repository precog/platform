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

import org.specs2.mutable._

import spire.ArrayOps
import spire.implicits._

import scala.util.Random

import blueeyes.json._

import scalaz._

trait ClusteringLibSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with ClusteringLib[M]
    with ClusteringTestSupport
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

  val ClusterIdPattern = """Cluster\d+""".r

  def isGoodCluster(clusterMap: Map[String, SValue], points: Array[Array[Double]], centers: Array[Array[Double]], k: Int, dimension: Int) = {
    val targetCost = kMediansCost(points, centers)

    clusterMap must haveSize(k)
    clusterMap.keys must haveAllElementsLike {
      case ClusterIdPattern(_) => ok
    }

    def getPoint(sval: SValue): List[Double] = sval match {
      case SArray(arr) =>
        arr.collect({ case SDecimal(n) => n.toDouble }).toList
      case SObject(obj) =>
        obj.toList.sortBy(_._1) flatMap { case (_, v) => getPoint(v) }
      case _ => sys.error("not supported")
    }

    val clusters: Array[Array[Double]] = clusterMap.values.map({ sval =>
      val arr = getPoint(sval)
      arr must haveSize(dimension)
      arr.toArray
    }).toArray

    val cost = kMediansCost(points, clusters)
    
    cost must be_<(3 * targetCost)
  }

  "k-medians clustering" should {
    "compute trivial k-medians clustering" in {
      val dimension = 4
      val k = 5
      val GeneratedPointSet(points, centers) = genPoints(2000, dimension, k)

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
            case SObject(clusterMap) => isGoodCluster(clusterMap, points, centers, k, dimension)
          }
        }
      }
    }

    "return result when given one row, with k > 1" in {
      val k = 5
      val clusterIds = (1 to k).map("Cluster" + _).toSet

      val line = Line(0, "")

      val input = dag.Morph2(line, KMediansClustering,
        dag.Const(line, CDouble(4.4)),
        dag.Const(line, CLong(k)))

      val result = testEval(input)

      result must haveSize(1)

      result must haveAllElementsLike { case (ids, SObject(obj)) if ids.size == 0 => 
        obj.keys mustEqual Set("Model1")
        obj("Model1") must beLike { 
          case SObject(clusterMap) => 
            clusterMap.keys mustEqual clusterIds
            clusterIds.map(clusterMap(_)) must haveAllElementsLike { 
              case SDecimal(d) => d mustEqual(4.4)
            } 
        }
      }
    }

    "return result when given fewer than k numeric rows" in {
      val k = 8
      val clusterIds = (1 to k).map("Cluster" + _).toSet
      val dataset = "/hom/numbers"

      val line = Line(0, "")

      val input = dag.Morph2(line, KMediansClustering,
        dag.LoadLocal(line, dag.Const(line, CString(dataset))),
        dag.Const(line, CLong(k)))

      val numbers = dag.LoadLocal(line, dag.Const(line, CString(dataset)))

      val result = testEval(input)
      val resultNumbers = testEval(numbers)

      result must haveSize(1)

      val expected = resultNumbers collect { case (_, SDecimal(num)) => num }

      result must haveAllElementsLike { case (ids, SObject(obj)) if ids.size == 0 => 
        obj.keys mustEqual Set("Model1")
        obj("Model1") must beLike { 
          case SObject(clusterMap) => 
            clusterMap.keys mustEqual clusterIds
            clusterIds.map(clusterMap(_)) must haveAllElementsLike { 
              case SDecimal(d) => expected must contain(d)
            } 

            val actual = clusterIds.map(clusterMap(_)) collect { case SDecimal(d) => d }
            actual.toSet mustEqual expected
        }
      }
    }

    "return result when given fewer than k rows, where the rows are objects" in {
      val k = 8
      val clusterIds = (1 to k).map("Cluster" + _).toSet
      val dataset = "/hom/heightWeight"

      val line = Line(0, "")

      val input = dag.Morph2(line, KMediansClustering,
        dag.LoadLocal(line, dag.Const(line, CString(dataset))),
        dag.Const(line, CLong(k)))

      val data = dag.LoadLocal(line, dag.Const(line, CString(dataset)))

      val result = testEval(input)
      val resultData = testEval(data)

      result must haveSize(1)

      val expected = resultData collect { case (_, SObject(obj)) => obj }

      result must haveAllElementsLike { case (ids, SObject(obj)) if ids.size == 0 => 
        obj.keys mustEqual Set("Model1")
        obj("Model1") must beLike { 
          case SObject(clusterMap) => 
            clusterMap.keys mustEqual clusterIds
            clusterIds.map(clusterMap(_)) must haveAllElementsLike { 
              case SObject(obj) => expected must contain(obj)
            } 

            val actual = clusterIds.map(clusterMap(_)) collect { case SObject(obj) => obj }
            actual.toSet mustEqual expected
        }
      }
    }

    "compute k-medians clustering with two distinct schemata" in {
      val dimensionA = 4
      val dimensionB = 12
      val k = 15

      val GeneratedPointSet(pointsA, centersA) = genPoints(5000, dimensionA, k)
      val GeneratedPointSet(pointsB, centersB) = genPoints(5000, dimensionB, k)

      val jvalsA = pointsToJson(pointsA) map { v => JObject(JField("a", v)) }
      val jvalsB = pointsToJson(pointsB) map { v => JObject(JField("b", v)) }
      val jvals = Random.shuffle(jvalsA ++ jvalsB)

      writeJValuesToDataset(jvals) { dataset =>
        val line = Line(0, "")

        val input = dag.Morph2(line, KMediansClustering,
          dag.LoadLocal(line, Const(line, CString(dataset))),
          dag.Const(line, CLong(k)))

        val result = testEval(input)

        result must haveSize(1)

        result must haveAllElementsLike { case (ids, SObject(obj)) if ids.size == 0 => 
          obj.keys mustEqual Set("Model1", "Model2")
          
          def checkModel(model: SValue) = model must beLike {
            case SObject(clusterMap) =>
              clusterMap("Cluster1") must beLike { case SObject(schemadCluster) =>
                if (schemadCluster contains "a") {
                  isGoodCluster(clusterMap, pointsA, centersA, k, dimensionA)
                } else {
                  isGoodCluster(clusterMap, pointsB, centersB, k, dimensionB)
                }
              }
          }

          checkModel(obj("Model1"))
          checkModel(obj("Model2"))
        }
      }
    }

    "compute k-medians clustering with overlapping schemata" in {
      val dimension = 6
      val k = 20

      val GeneratedPointSet(pointsA, centersA) = genPoints(5000, dimension, k)

      val points = pointsA.zipWithIndex map {
        case (p, i) if i % 3 == 2 => p ++ Array.fill(3)(Random.nextDouble)
        case (p, _) => p
      }

      writePointsToDataset(points) { dataset =>
        val line = Line(0, "")

        val input = dag.Morph2(line, KMediansClustering,
          dag.LoadLocal(line, Const(line, CString(dataset))),
          dag.Const(line, CLong(k)))

        val result = testEval(input)

        result must haveSize(1)

        result must haveAllElementsLike { case (ids, SObject(obj)) if ids.size == 0 => 
          obj.keys mustEqual Set("Model1", "Model2")
          
          def checkModel(model: SValue) = model must beLike {
            case SObject(clusterMap) =>
              clusterMap("Cluster1") must beLike {
                case SArray(arr) if arr.size == 6 =>
                  isGoodCluster(clusterMap, pointsA, centersA, k, dimension)
                case SArray(arr) if arr.size == 9 =>
                  ok
              }
          }

          checkModel(obj("Model1"))
          checkModel(obj("Model2"))
        }
      }
    }
  }
}

object ClusteringLibSpecs extends ClusteringLibSpecs[test.YId] with test.YIdInstances
