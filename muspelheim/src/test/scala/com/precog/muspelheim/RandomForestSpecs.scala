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

import yggdrasil._

trait RandomForestSpecs extends EvalStackSpecs {

  "random forest" should {
    "return correctly structured classification results" in {
      val input = """
        data := //iris
        trainingData := {predictors: data.features, dependent: data.species}

        std::stats::rfClassification(trainingData, data.features)
      """

      val results = evalE(input)

      val categories = Set("Iris-setosa","Iris-versicolor", "Iris-virginica") 

      results must haveSize(150)

      results must haveAllElementsLike { case (ids, value) =>
        ids must haveSize(1)
        value must beLike { case SObject(obj) => 
          obj.keySet mustEqual Set("model1")
          obj("model1") must beLike { case SString(str) =>
            categories must contain(str)
          }
        }
      }
    }

    "return correctly structured regression results" in {
      val input = """
        data := //auto-mpg
        features0 := data.features

        features :=
          [
            features0[0],
            features0[1],
            if (features0[2] = null) then mean(features0[2]) else features0[2],
            features0[3],
            features0[4],
            features0[5]
          ]

        trainingData := {predictors: features, dependent: data.mpg}

        std::stats::rfRegression(trainingData, features)
      """

      val results = evalE(input)

      results must haveSize(398)

      results must haveAllElementsLike { case (ids, value) =>
        ids must haveSize(1)
        value must beLike { case SObject(obj) => 
          obj.keySet mustEqual Set("model1")
          obj("model1") must beLike { case SDecimal(_) => ok }
        }
      }
    }

    "return correctly structured classification results given dependent object" in {
      val input = """
        data := //iris
        trainingData := {predictors: data.features, dependent: {species: data.species}}

        std::stats::rfClassification(trainingData, data.features)
      """

      val results = evalE(input)

      val categories = Set("Iris-setosa","Iris-versicolor", "Iris-virginica") 

      results must haveSize(150)

      results must haveAllElementsLike { case (ids, value) =>
        ids must haveSize(1)
        value must beLike { case SObject(obj) => 
          obj.keySet mustEqual Set("model1")
          obj("model1") must beLike { case SObject(pred) =>
            pred.keySet mustEqual Set("species")
            pred("species") must beLike { case SString(str) =>
              categories must contain(str)
            }
          }
        }
      }
    }

    "return empty set in classification case when given wrongly structured data" in {
      val input = """
        data := //iris
        std::stats::rfClassification(data.species, data.features)
      """

      evalE(input) must beEmpty
    }

    "return empty set in regression case when given wrongly structured data" in {
      val input = """
        data := //iris
        std::stats::rfRegression(data.species, data.features)
      """

      evalE(input) must beEmpty
    }

    "handle single datapoint in regression case" in {
      val input = """
        trainingData := {predictors: [1, 2, 3.3, 5], dependent: 0.25}
        std::stats::rfRegression(trainingData, [3, 4.9, 5, 1])
      """

      val results = evalE(input)

      results must haveSize(1)

      results must haveAllElementsLike { case (ids, value) =>
        ids must haveSize(0)
        value must beLike { case SObject(obj) =>
          obj.keySet mustEqual Set("model1")
          obj("model1") must beLike { case SDecimal(d) =>
            d.toDouble mustEqual(0.25)
          }
        }
      }
    }

    "return well-predicted classification results" in {
      val input = """
        data0 := //iris
        data := data0 with { rand: observe(data0, std::random::uniform(42)) }

        pt := 0.9

        trainingData0 := data where data.rand <= pt
        trainingData := {predictors: trainingData0.features, dependent: trainingData0.species}

        predictionData := data where data.rand > pt 

        predictions := std::stats::rfClassification(trainingData, predictionData.features)

        results := predictionData with predictions

        correct := count(results where results.species = results.model1)
        total := count(results)

        correct / total
      """
      val results = evalE(input)

      results must haveSize(1)

      results must haveAllElementsLike { case (ids, value) =>
        ids must haveSize(0)
        value must beLike { case SDecimal(d) =>
          //println("pred rate classification: " + d.toDouble)
          d.toDouble must be_>(0.5)
        }
      }
    }

    "return well-predicted regression results" in {
      val input = """
        data0 := //auto-mpg
        features0 := data0.features

        countryOfOrigin := distinct(features0[6])

        features0 ~ countryOfOrigin

        features :=
          [
            features0[0],
            features0[1],
            if (features0[2] = null) then mean(features0[2]) else features0[2],
            features0[3],
            features0[4],
            features0[5],
            std::stats::dummy(countryOfOrigin) where countryOfOrigin = features0[6]
          ]

         data := { predictors: features, dependent: data0.mpg }
         rng := observe(data0, std::random::uniform(112))

         pt := 0.9
         trainingData := data where rng <= pt
         predictionData := data where rng > pt

         predictions := std::stats::rfRegression(trainingData, predictionData.predictors)
         results := predictionData with predictions

         SSerr := sum((results.model1 - results.dependent)^2)
         SStot := sum((results.dependent - mean(results.dependent))^2)

         1 - (SSerr / SStot)
      """

      val results = evalE(input)

      results must haveSize(1)

      results must haveAllElementsLike { case (ids, value) =>
        ids must haveSize(0)
        value must beLike { case SDecimal(d) => 
          //println("r^2 regression: " + d)
          d.toDouble must be_>(0.4)
        }
      }
    }
  }
}
