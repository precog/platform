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

      forall(results) {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("Model1")
          elems("Model1") must beLike { case SObject(clusters) =>
            clusters must haveSize(3)
            clusterSchema(clusters) must_== List("height", "weight")
          }
        case _ => ko
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

      forall(results) {
        case (ids, SObject(elems)) =>
          ids must haveSize(1)
          elems.keys mustEqual Set("Model1")

          elems("Model1") must beLike {
            case SString(c1) => validClusters must contain(c1)
          }
        case _ => ko
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

      forall(results) {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("Model1", "Model2", "Model3", "Model4")
        case _ => ko
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

      forall(results) {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("Model1")
          elems("Model1") must beLike {
            case SObject(obj) => obj.keySet mustEqual (1 to 6).map("Cluster" + _).toSet
          }
        case _ => ko
      }
    }
  }
}
