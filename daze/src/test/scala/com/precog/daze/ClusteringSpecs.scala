package com.precog.daze

import com.precog.common.Path
import com.precog.yggdrasil._

import org.specs2.mutable._

import spire.ArrayOps
import spire.implicits._

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

  "k-medians clustering" should {
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
