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

import com.precog.common.Path
import com.precog.yggdrasil._

import blueeyes.json._

import org.specs2.mutable._

import spire.implicits._

import scalaz._

trait AssignClustersSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with ClusteringTestSupport
    with AssignClustersLib[M]
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

  def assign(points: Array[Array[Double]], centers: Array[Array[Double]]): Map[JValue, String] = {
    points.map { p =>
      val id = (0 until centers.length) minBy { i => (p - centers(i)).norm }
      pointToJson(p) -> ("Cluster" + (id +  1))
    }.toMap
  }

  def makeClusters(centers: Array[Array[Double]]) = {
    JObject(pointsToJson(centers).zipWithIndex map { case (ctr, idx) => 
      JField("Cluster" + (idx + 1), ctr)
    })
  }

  def createDAG(pointsDataSet: String, modelDataSet: String) = {
    val line = Line(0, "")

    val points = dag.LoadLocal(line, Const(line, CString(pointsDataSet)))

    val input = dag.Morph2(line, AssignClusters,
      points,
      dag.LoadLocal(line, Const(line, CString(modelDataSet))))

    dag.Join(line, JoinObject, IdentitySort,
      input,
      dag.Join(line, WrapObject, CrossLeftSort,
        Const(line, CString("point")),
        points))
  }

  "assign clusters" should {
    "assign correctly with a single schema" in {
      val GeneratedPointSet(points, centers) = genPoints(3000, 4, 8)

      val clusters = makeClusters(centers)
      
      val model1 = JObject(JField("Model1", clusters) :: Nil)
      val assignments = assign(points, centers)

      writeJValuesToDataset(List(model1)) { modelDataSet =>
        writePointsToDataset(points) { pointsDataSet =>
          val input2 = createDAG(pointsDataSet, modelDataSet)
          val result = testEval(input2)

          result must haveAllElementsLike { case (ids, SObject(obj)) =>
            ids.length mustEqual 2
            obj.keySet mustEqual Set("point", "Model1")
            val point = obj("point")
            obj("Model1") must beLike { case SString(clusterId) =>
              clusterId must_== assignments(point.toJValue)
            }
          }
        }
      }
    }

    "assign correctly with two distinct schemata" in {
      val dimensionA = 4
      val dimensionB = 12
      val k = 15

      val GeneratedPointSet(pointsA, centersA) = genPoints(5000, dimensionA, k)
      val GeneratedPointSet(pointsB, centersB) = genPoints(5000, dimensionB, k)

      val points = Random.shuffle(pointsA.toList ++ pointsB.toList).toArray

      val clustersA = makeClusters(centersA)
      val clustersB = makeClusters(centersB)

      val model1 = JObject(JField("Model1", clustersA) :: JField("Model2", clustersB) :: Nil)

      val assignmentsA = assign(pointsA, centersA)
      val assignmentsB = assign(pointsB, centersB)

      writeJValuesToDataset(List(model1)) { modelDataSet =>
        writePointsToDataset(points) { pointsDataSet =>
          val input2 = createDAG(pointsDataSet, modelDataSet)
          val result = testEval(input2)

          result must haveAllElementsLike { case (ids, SObject(obj)) =>
            ids.length mustEqual 2

            (obj.keySet mustEqual Set("point", "Model1")) or 
              (obj.keySet mustEqual Set("point", "Model2"))

            val point = obj("point")
            obj("Model1") must beLike { case SString(clusterId) =>
              clusterId must_== assignmentsA(point.toJValue)
            }
            obj("Model2") must beLike { case SString(clusterId) =>
              clusterId must_== assignmentsB(point.toJValue)
            }
          }
        }
      }
    }

    "assign correctly with two overlapping schemata" in {
      val dimension = 6
      val k = 20

      val GeneratedPointSet(points0, centers) = genPoints(5000, dimension, k)

      val points = points0.zipWithIndex map {
        case (p, i) if i % 3 == 2 => p ++ Array.fill(3)(Random.nextDouble)
        case (p, _) => p
      }

      val clusters = makeClusters(centers)

      val model1 = JObject(JField("Model1", clusters) :: Nil)

      val assignments = assign(points0, centers)

      writeJValuesToDataset(List(model1)) { modelDataSet =>
        writePointsToDataset(points) { pointsDataSet =>
          val input2 = createDAG(pointsDataSet, modelDataSet)
          val result = testEval(input2)

          result must haveAllElementsLike { case (ids, SObject(obj)) =>
            ids.length mustEqual 2

            (obj.keySet mustEqual Set("point", "Model1"))

            val point = obj("point")
            obj("Model1") must beLike { case SString(clusterId) =>
              clusterId must_== assignments(point.toJValue)
            }
          }
        }
      }
    }

    "assign correctly with multiple rows of schema with overlapping modelIds" in {
      val line = Line(0, "")

      val input = dag.Morph2(line, AssignClusters,
        dag.LoadLocal(line, Const(line, CString("/hom/clusteringData"))),
        dag.LoadLocal(line, Const(line, CString("/hom/clusteringModel"))))

      val result0 = testEval(input)

      result0 must haveSize(7)

      val result = result0 collect { case (ids, value) if ids.size == 2 => value }

      result mustEqual Set(
        (SObject(Map("Model1" -> SString("Cluster2")))), 
        (SObject(Map("Model2" -> SString("Cluster1")))),
        (SObject(Map("Model2" -> SString("Cluster1")))), 
        (SObject(Map("Model1" -> SString("Cluster2")))), 
        (SObject(Map("Model1" -> SString("Cluster2")))), 
        (SObject(Map("Model1" -> SString("Cluster3")))), 
        (SObject(Map("Model1" -> SString("Cluster1"))))) 
    }
  }
}

object AssignClustersSpecs extends AssignClustersSpecs[test.YId] with test.YIdInstances
