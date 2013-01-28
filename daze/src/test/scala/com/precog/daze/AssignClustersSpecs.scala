package com.precog.daze

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

  "assign clusters" should {
    "assign correctly" in {
      val pointsAndCenters = genPoints(3000, 4, 8)
      val GeneratedPointSet(points, centers) = pointsAndCenters

      val clusters = JObject(pointsToJson(centers).zipWithIndex map { case (ctr, idx) => 
        JField("Cluster" + (idx + 1), ctr)
      })
      
      val model1 = JObject(JField("Model1", clusters) :: Nil)
      val assignments = assign(points, centers)

      writeJValuesToDataset(List(model1)) { modelDataSet =>
        writePointsToDataset(points) { pointsDataSet =>
          val line = Line(0, "")

          val points = dag.LoadLocal(line, Const(line, CString(pointsDataSet)))

          val input = dag.Morph2(line, AssignClusters,
            dag.LoadLocal(line, Const(line, CString(modelDataSet))),
            points)

         val input2 = dag.Join(line, JoinObject, IdentitySort,
           input,
           dag.Join(line, WrapObject, CrossLeftSort,
             Const(line, CString("point")),
             points))

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
  }
}

object AssignClustersSpecs extends AssignClustersSpecs[test.YId] with test.YIdInstances
