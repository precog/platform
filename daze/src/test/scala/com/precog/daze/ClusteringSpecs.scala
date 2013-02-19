package com.precog.daze

import com.precog.common._
import com.precog.yggdrasil._

import org.specs2.mutable._

import spire.ArrayOps
import spire.implicits._

import scala.util.Random

import blueeyes.json._

import scalaz._

trait ClusteringLibSpecs[M[+_]] extends Specification
    with EvaluatorTestSupport[M]
    with ClusteringTestSupport
    with LongIdMemoryDatasetConsumer[M]{ self =>

  import dag._
  import instructions._
  import library._

  val testAPIKey = "testAPIKey"
  val line = Line(0, 0, "")

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

  def clusterInput(dataset: String, k: Long) = {
    dag.Morph2(KMediansClustering,
      dag.LoadLocal(Const(CString(dataset))(line))(line),
      dag.Const(CLong(k))(line)
    )(line)
  }

  "k-medians clustering" should {
    "compute trivial k-medians clustering" in {
      val dimension = 4
      val k = 5
      val GeneratedPointSet(points, centers) = genPoints(2000, dimension, k)

      writePointsToDataset(points) { dataset =>
        val input = clusterInput(dataset, k)
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

      val input = dag.Morph2(KMediansClustering,
        dag.Const(CNum(4.4))(line),
        dag.Const(CLong(k))(line)
      )(line)

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

      val input = clusterInput(dataset, k)

      val numbers = dag.LoadLocal(dag.Const(CString(dataset))(line))(line)

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

      val input = clusterInput(dataset, k)

      val data = dag.LoadLocal(dag.Const(CString(dataset))(line))(line)

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

      val jvalsA = pointsToJson(pointsA) map { v => RObject(Map("a" -> v)) }
      val jvalsB = pointsToJson(pointsB) map { v => RObject(Map("b" -> v)) }
      val jvals = Random.shuffle(jvalsA ++ jvalsB)

      writeRValuesToDataset(jvals) { dataset =>
        val input = clusterInput(dataset, k)

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
        val input = clusterInput(dataset, k)

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

  def assign(points: Array[Array[Double]], centers: Array[Array[Double]]): Map[RValue, String] = {
    points.map { p =>
      val id = (0 until centers.length) minBy { i => (p - centers(i)).norm }
      pointToJson(p) -> ("Cluster" + (id +  1))
    }.toMap
  }

  def makeClusters(centers: Array[Array[Double]]) = {
    RObject(pointsToJson(centers).zipWithIndex.map { case (ctr, idx) => 
      ("Cluster" + (idx + 1), ctr)
    }.toMap)
  }

  def createDAG(pointsDataSet: String, modelDataSet: String) = {
    val points = dag.LoadLocal(Const(CString(pointsDataSet))(line))(line)

    val input = dag.Morph2(AssignClusters,
      points,
      dag.LoadLocal(Const(CString(modelDataSet))(line))(line)
    )(line)

    dag.Join(JoinObject, IdentitySort,
      input,
      dag.Join(WrapObject, CrossLeftSort,
        Const(CString("point"))(line),
        points)(line))(line)
  }

  "assign clusters" should {
    "assign correctly with a single schema" in {
      val GeneratedPointSet(points, centers) = genPoints(3000, 4, 8)

      val clusters = makeClusters(centers)
      
      val model1 = RObject(Map("Model1" -> clusters))
      val assignments = assign(points, centers)

      writeRValuesToDataset(List(model1)) { modelDataSet =>
        writePointsToDataset(points) { pointsDataSet =>
          val input2 = createDAG(pointsDataSet, modelDataSet)
          val result = testEval(input2)

          result must haveAllElementsLike { case (ids, SObject(obj)) =>
            ids.length mustEqual 2
            obj.keySet mustEqual Set("point", "Model1")
            val point = obj("point")
            obj("Model1") must beLike { case SString(clusterId) =>
              clusterId must_== assignments(point.toRValue)
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

      val model1 = RObject(Map("Model1" -> clustersA, "Model2" -> clustersB))

      val assignmentsA = assign(pointsA, centersA)
      val assignmentsB = assign(pointsB, centersB)

      writeRValuesToDataset(List(model1)) { modelDataSet =>
        writePointsToDataset(points) { pointsDataSet =>
          val input2 = createDAG(pointsDataSet, modelDataSet)
          val result = testEval(input2)

          result must haveAllElementsLike { case (ids, SObject(obj)) =>
            ids.length mustEqual 2

            (obj.keySet mustEqual Set("point", "Model1")) or 
              (obj.keySet mustEqual Set("point", "Model2"))

            val point = obj("point")
            obj("Model1") must beLike { case SString(clusterId) =>
              clusterId must_== assignmentsA(point.toRValue)
            }
            obj("Model2") must beLike { case SString(clusterId) =>
              clusterId must_== assignmentsB(point.toRValue)
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

      val model1 = RObject(Map("Model1" -> clusters))

      val assignments = assign(points0, centers)

      writeRValuesToDataset(List(model1)) { modelDataSet =>
        writePointsToDataset(points) { pointsDataSet =>
          val input2 = createDAG(pointsDataSet, modelDataSet)
          val result = testEval(input2)

          result must haveAllElementsLike { case (ids, SObject(obj)) =>
            ids.length mustEqual 2

            (obj.keySet mustEqual Set("point", "Model1"))

            val point = obj("point")
            obj("Model1") must beLike { case SString(clusterId) =>
              clusterId must_== assignments(point.toRValue)
            }
          }
        }
      }
    }

    "assign correctly with multiple rows of schema with overlapping modelIds" in {
      val input = dag.Morph2(AssignClusters,
        dag.LoadLocal(Const(CString("/hom/clusteringData"))(line))(line),
        dag.LoadLocal(Const(CString("/hom/clusteringModel"))(line))(line)
      )(line)

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

object ClusteringLibSpecs extends ClusteringLibSpecs[test.YId] with test.YIdInstances
