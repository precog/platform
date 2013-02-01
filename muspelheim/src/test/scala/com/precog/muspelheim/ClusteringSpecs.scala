package com.precog
package muspelheim

import com.precog.yggdrasil._

trait ClusteringSpecs extends EvalStackSpecs {

  def clusterSchema(obj: Map[String, SValue]): List[String] = obj("Cluster1") match {
    case SObject(ctr) => ctr.keys.toList.sorted
    case _ => sys.error("malformed SObject")
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
          elems.keys mustEqual Set("Model1")
          elems("Model1") must beLike { case SObject(clusters) =>
            clusters must haveSize(3)
            clusters.keys mustEqual Set("Cluster1", "Cluster2", "Cluster3")
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

      val validClusters = (1 to 4).map("Cluster" + _).toSet

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(1)
          elems.keys mustEqual Set("Model1")

          elems("Model1") must beLike {
            case SString(c1) => validClusters must contain(c1)
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

      val results = evalE(input)
      val resultsCount = evalE(input2)

      val count = resultsCount.collectFirst { case (_, SDecimal(d)) => d.toInt }.get

      results must haveSize(count)
      results must not beEmpty

      val validClusters = (1 to 4).map("Cluster" + _).toSet

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          elems.keys must contain("cluster")
          elems("cluster") must beLike {
            case SObject(obj) =>
              obj.keys mustEqual Set("Model1")
              obj("Model1") must beLike {
                case SString(clusterId) => validClusters must contain(clusterId)
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

        medals' := medals with { cluster: assignments.Model1 }

        solve 'cluster
          clusters := medals'.cluster where medals'.cluster = 'cluster
          { numPtsInCluster: count(clusters), clusterId: 'cluster }
      """.stripMargin

      val results = evalE(input)

      results must haveSize(5)

      val validClusters = (1 to 5).map("Cluster" + _).toSet

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          elems.keys mustEqual Set("numPtsInCluster", "clusterId") 
          elems("numPtsInCluster") must beLike {
            case SDecimal(d) => d must be_>=(BigDecimal(1))
          }
          elems("clusterId") must beLike {
            case SString(clusterId) => validClusters must contain(clusterId)
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
          elems.keys mustEqual Set("Model1", "Model2", "Model3", "Model4")
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
          elems.keys mustEqual Set("Model1")
          elems("Model1") must beLike {
            case SObject(obj) => obj.keySet mustEqual (1 to 6).map("Cluster" + _).toSet
          }
      }
    }
  }
}
