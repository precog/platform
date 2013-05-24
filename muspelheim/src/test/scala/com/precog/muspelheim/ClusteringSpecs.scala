package com.precog
package muspelheim

import com.precog.yggdrasil._
import com.precog.common._

trait ClusteringSpecs extends EvalStackSpecs {
  import stack._

  def clusterSchema(obj: Map[String, SValue]): List[String] = obj("cluster1") match {
    case SObject(ctr) => ctr.keys.toList.sorted
    case _ => sys.error("malformed SObject")
  }

  def testmodel(model: Map[String, SValue], validClusters: Set[String]) = {
    model.keys mustEqual Set("clusterId", "clusterCenter")

    model("clusterId") must beLike {
      case SString(c1) => validClusters must contain(c1)
    }
    model("clusterCenter") must beLike {
      case SObject(v) => v.keys mustEqual Set("HeightIncm", "Weight")
    }
  }
  
  "clustering" should {
    "return correctly structured results in simple case" in {
      val input = """
          medals := //summer_games/london_medals
          h := medals.HeightIncm where std::type::isNumber(medals.HeightIncm)
          w := medals.Weight where std::type::isNumber(medals.Weight)
          std::stats::kMedians({ height: h, weight: w }, 3)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(1)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")
          elems("model1") must beLike { case SObject(clusters) =>
            clusters must haveSize(3)
            clusters.keys mustEqual Set("cluster1", "cluster2", "cluster3")
            clusterSchema(clusters) must_== List("height", "weight")
          }
      }
    }

    "assign values to a cluster" in {
      val input = """
          medals := //summer_games/london_medals

          h := medals.HeightIncm where std::type::isNumber(medals.HeightIncm)
          w := medals.Weight where std::type::isNumber(medals.Weight)

          clustering := std::stats::kMedians({ HeightIncm: h, Weight: w }, 4)

          std::stats::assignClusters(medals, clustering)
        """.stripMargin

      val input2 = """ 
        medals := //summer_games/london_medals

        h := medals.HeightIncm where std::type::isNumber(medals.HeightIncm)
        w := medals.Weight where std::type::isNumber(medals.Weight)

        count({ height: h, weight: w })
      """

      val results = evalE(input)
      val resultsCount = evalE(input2)

      val count = resultsCount.collectFirst { case (_, SDecimal(d)) => d.toInt }.get
      results must haveSize(count)

      val validClusters = (1 to 4).map("cluster" + _).toSet

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(1)
          elems.keys mustEqual Set("model1")

          elems("model1") must beLike {
            case SObject(model) => testmodel(model, validClusters)
          }
      }
    }

    def testJoinCluster(input: String, input2: String, idJoin: Boolean) = {
      val results = evalE(input)
      val resultsCount = evalE(input2)

      val count = resultsCount.collectFirst { case (_, SDecimal(d)) => d.toInt }.get

      results must haveSize(count)
      results must not beEmpty

      val validClusters = (1 to 3).map("cluster" + _).toSet

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          if (idJoin) ids must haveSize(2)
          else ids must haveSize(1)

          elems.keys must contain("cluster")
          elems("cluster") must beLike {
            case SObject(obj) =>
              obj.keys mustEqual Set("model1")

              obj("model1") must beLike {
                case SObject(model) => testmodel(model, validClusters)
              }
          }
      }
    }

    "join cluster information to original data" in {
      val input = """
        medals := //summer_games/london_medals

        h := medals.HeightIncm where std::type::isNumber(medals.HeightIncm)
        w := medals.Weight where std::type::isNumber(medals.Weight)

        clustering := std::stats::kMedians({ HeightIncm: h, Weight: w }, 3)
        assignments := std::stats::assignClusters(medals, clustering)

        medals with { cluster: assignments }
        --assignments with { points: medals }
      """.stripMargin

      val input2 = """ 
        medals := //summer_games/london_medals

        h := medals.HeightIncm where std::type::isNumber(medals.HeightIncm)
        w := medals.Weight where std::type::isNumber(medals.Weight)

        count({ height: h, weight: w })
      """

      testJoinCluster(input, input2, false)
    }

    "join cluster information to original data when clustering is `new`ed" in {
      val input = """
        medals := //summer_games/london_medals

        h := medals.HeightIncm where std::type::isNumber(medals.HeightIncm)
        w := medals.Weight where std::type::isNumber(medals.Weight)

        clustering := new std::stats::kMedians({ HeightIncm: h, Weight: w }, 3)

        medals ~ clustering
        assignments := std::stats::assignClusters(medals, clustering)

        medals with { cluster: assignments }
      """.stripMargin

      val input2 = """ 
        medals := //summer_games/london_medals

        h := medals.HeightIncm where std::type::isNumber(medals.HeightIncm)
        w := medals.Weight where std::type::isNumber(medals.Weight)

        count({ height: h, weight: w })
      """

      testJoinCluster(input, input2, true)
    }

    "join cluster information to clustering when clustering is `new`ed" in {
      val input = """
        medals := //summer_games/london_medals

        h := medals.HeightIncm where std::type::isNumber(medals.HeightIncm)
        w := medals.Weight where std::type::isNumber(medals.Weight)

        clustering := new std::stats::kMedians({ HeightIncm: h, Weight: w }, 3)

        medals ~ clustering
        assignments := std::stats::assignClusters(medals, clustering)

        clustering with { cluster: assignments }
      """.stripMargin

      val input2 = """ 
        medals := //summer_games/london_medals

        h := medals.HeightIncm where std::type::isNumber(medals.HeightIncm)
        w := medals.Weight where std::type::isNumber(medals.Weight)

        count({ height: h, weight: w })
      """

      val results = evalE(input)
      val resultsCount = evalE(input2)

      val count = resultsCount.collectFirst { case (_, SDecimal(d)) => d.toInt }.get

      results must haveSize(count)
      results must not beEmpty

      val validClusters = (1 to 3).map("cluster" + _).toSet

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(2)

          elems.keys mustEqual Set("cluster", "model1")

          elems("model1") must beLike {
            case SObject(obj) =>
              obj.keys mustEqual(validClusters)
          }

          elems("cluster") must beLike {
            case SObject(obj) =>
              obj.keys mustEqual Set("model1")

              obj("model1") must beLike {
                case SObject(model) => testmodel(model, validClusters)
              }
          }
      }
    }

    "make a histogram of clusters" in {
      val input = """
        medals := //summer_games/london_medals

        h := medals.HeightIncm where std::type::isNumber(medals.HeightIncm)
        w := medals.Weight where std::type::isNumber(medals.Weight)

        clustering := std::stats::kMedians({ HeightIncm: h, Weight: w }, 5)
        assignments := std::stats::assignClusters(medals, clustering)

        medals' := medals with { cluster: assignments.model1 }

        solve 'cluster
          clusters := medals'.cluster where medals'.cluster = 'cluster
          { numPtsInCluster: count(clusters), clusterId: 'cluster }
      """.stripMargin

      val results = evalE(input)

      results must haveSize(5)

      val validClusters = (1 to 5).map("cluster" + _).toSet

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          elems.keys mustEqual Set("numPtsInCluster", "clusterId") 
          elems("numPtsInCluster") must beLike {
            case SDecimal(d) => d must be_>=(BigDecimal(1))
          }
          elems("clusterId") must beLike {
            case SObject(model) => testmodel(model, validClusters)
          }
      }
    }

    "assign values to a cluster when field names of cluster aren't present in data" in {
      val input = """
          medals := //summer_games/london_medals
          clustering := std::stats::kMedians({ foo: medals.HeightIncm, bar: medals.Weight }, 4)

          std::stats::assignClusters(medals, clustering)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(0)
    }

    "return correct number of results in more complex case" in {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::kMedians(medals, 9)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(1)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("model1", "model2", "model3", "model4")
      }
    }

    "return nothing when fed empty set" in {
      val input = """
          foobar := //foobar
          
          std::stats::kMedians(foobar, 9)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(0)
    }

    "return nothing when k = 0" in {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::kMedians(medals, 0)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(0)
    }

    "return nothing when features of wrong type" in {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::kMedians(medals.Country, 5)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(0)
    }

    "return nothing when k is of wrong type" in {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::kMedians(medals.Weight, "34")
        """.stripMargin

      val results = evalE(input)

      results must haveSize(0)
    }

    "return nothing when k is negative" in {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::kMedians(medals, neg 23)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(0)
    }

    "return points when the number of rows < k" in {
      val input = """
          std::stats::kMedians([3, 4, 5], 6)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(1)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")
          elems("model1") must beLike {
            case SObject(obj) => obj.keySet mustEqual (1 to 6).map("cluster" + _).toSet
          }
      }
    }
    
    "evaluate an invalid clustering query without exploding" in {
      val input = """
        | locations := //devices
        | 
        | model := std::stats::kMedians({x: locations.x, y: locations.y }, 5)
        | std::stats::assignClusters(model, locations)
        | """.stripMargin
        
      eval(input) must beEmpty
    }
    
    "evaluate an valid clustering query without exploding" in {
      val input = """
        | locations := //devices
        | 
        | model := std::stats::kMedians({x: locations.x, y: locations.y }, 5)
        | std::stats::assignClusters(locations, model)
        | """.stripMargin
        
      eval(input) must not(beEmpty)
    }
  }
}
