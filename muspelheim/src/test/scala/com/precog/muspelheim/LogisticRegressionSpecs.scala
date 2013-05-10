package com.precog
package muspelheim

import com.precog.yggdrasil._

trait LogisticRegressionSpecs extends EvalStackSpecs {
  "logistic regression" should {
    "return correctly structured results in simple case of logistic regression" in {
      val input = """
          medals := //summer_games/london_medals
          gender := (1 where medals.Sex = "F") union (0 where medals.Sex = "M")
          
          std::stats::logisticRegression(gender, { height: medals.HeightIncm })
        """.stripMargin

      val results = evalE(input)

      results must haveSize(1)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          val SObject(fields) = elems("model1")
          val SArray(arr) = fields("coefficients")
          
          arr(0) must beLike { case SObject(elems) =>
            elems.keys mustEqual Set("height")
            
            elems("height") must beLike { case SObject(obj) =>
              obj("estimate") must beLike { case SDecimal(d) =>
                elems must haveSize(1)
              }
            }
          }
          arr(1) must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("estimate")

            obj("estimate") must beLike { case SDecimal(d) => ok }
          }
      }
    }

    "predict logistic regression" in {
      val input = """
        medals := //summer_games/london_medals
        
        gender := (1 where medals.Sex = "F") union (0 where medals.Sex = "M")
        model := std::stats::logisticRegression(gender, { height: medals.HeightIncm })

        std::stats::predictLogistic({height: 34, other: 35}, model)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(1)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          elems("model1") must beLike { case SObject(obj) =>
            obj.keySet mustEqual Set("fit")
            obj("fit") must beLike { case SDecimal(_) => ok }
          }
        }
      }
    }

    def testJoinLogistic(input: String, input2: String, idJoin: Boolean) = {
      val results = evalE(input)
      val resultsCount = evalE(input2)

      val count = resultsCount.collectFirst { case (_, SDecimal(d)) => d.toInt }.get
      results must haveSize(count)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          if (idJoin) ids must haveSize(2)
          else ids must haveSize(1)

          elems.keys must contain("predictedGender")

          elems("predictedGender") must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("model1")
            obj("model1") must beLike { case SObject(obj2) =>
              obj2.keySet mustEqual Set("fit")
              obj2("fit") must beLike { case SDecimal(d) =>
                (d must be_>=(BigDecimal(0))) and (d must be_<=(BigDecimal(1)))
              }
            }
          }
        }
      }
    }

    //"join predicted results with original dataset" in {
    //  val input = """
    //    medals := //summer_games/london_medals
    //    
    //    gender := (1 where medals.Sex = "F") union (0 where medals.Sex = "M")
    //    model := std::stats::logisticRegression(gender, { HeightIncm: medals.HeightIncm })

    //    predictions := std::stats::predictLogistic(medals, model)

    //    medals with { predictedGender: predictions }
    //  """

    //  val input2 = """ 
    //    medals := //summer_games/london_medals

    //    h := medals where std::type::isNumber(medals.HeightIncm)
    //    count(h)
    //  """

    //  testJoinLogistic(input, input2, false)
    //}

    "join predicted results with original dataset when model is `new`ed" in {
      val input = """
        medals := //summer_games/london_medals
        
        gender := (1 where medals.Sex = "F") union (0 where medals.Sex = "M")
        model := new std::stats::logisticRegression(gender, { HeightIncm: medals.HeightIncm })

        medals ~ model
        predictions := std::stats::predictLogistic(medals, model)

        medals with { predictedGender: predictions }
      """

      val input2 = """ 
        medals := //summer_games/london_medals

        h := medals where std::type::isNumber(medals.HeightIncm)
        count(h)
      """

      testJoinLogistic(input, input2, true)
    }

    "join predicted results with model when model is `new`ed" in {
      val input = """
        medals := //summer_games/london_medals
        
        gender := (1 where medals.Sex = "F") union (0 where medals.Sex = "M")
        model := new std::stats::logisticRegression(gender, { HeightIncm: medals.HeightIncm })

        medals ~ model
        predictions := std::stats::predictLogistic(medals, model)

        model with { predictedGender: predictions }
      """

      val input2 = """ 
        medals := //summer_games/london_medals

        h := medals where std::type::isNumber(medals.HeightIncm)
        count(h)
      """

      val results = evalE(input)
      val resultsCount = evalE(input2)

      val count = resultsCount.collectFirst { case (_, SDecimal(d)) => d.toInt }.get
      results must haveSize(count)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(2)

          elems.keys mustEqual Set("model1", "predictedGender") 

          elems("predictedGender") must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("model1")
            obj("model1") must beLike { case SObject(obj2) =>
              obj2.keySet mustEqual Set("fit")
              obj2("fit") must beLike { case SDecimal(d) =>
                (d must be_>=(BigDecimal(0))) and (d must be_<=(BigDecimal(1)))
              }
            }
          }
        }
      }
    }

    "predict logistic regression when no field names in model are present in data" in {
      val input = """
        medals := //summer_games/london_medals
        
        gender := (1 where medals.Sex = "F") union (0 where medals.Sex = "M")
        model := std::stats::logisticRegression(gender, { height: medals.HeightIncm })

        std::stats::predictLogistic({baz: 34, bar: 35}, model)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(0)
    }

    "return correct number of results in more complex case of logistic regression" in {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::logisticRegression(medals.S, medals)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(1)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("model1", "model2", "model3", "model4")
      }
    }

    "return something when fed constants" in {
      val input = """
          std::stats::logisticRegression(0, 4)
        """.stripMargin

      val results = evalE(input)

      results must haveSize(1)
    }

    "return empty set when the classification variable is not at the root path" in {
      val input = """
               medals := //summer_games/london_medals
               medals' := medals with { gender: (1 where medals.Sex = "F") union (0 where medals.Sex = "M") }
               
               std::stats::logisticRegression({gender: medals'.gender}, {height: medals'.HeightIncm})
               """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when none of the classification values are 0 or 1" in {
      val input = """
               medals := //summer_games/london_medals
               medals' := medals with { gender: (1 where medals.Sex = "F") union (0 where medals.Sex = "M") }
               
               std::stats::logisticRegression(5, {height: medals'.HeightIncm})
               """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when given feature values of wrong type" in {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::logisticRegression(medals.WeightIncm, medals.Country)
        """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when given classication values of wrong type" in {
      val input = """
          medals := //summer_games/london_medals
          
          std::stats::logisticRegression(medals.Country, medals.WeightIncm)
        """.stripMargin

      evalE(input) must beEmpty
    }
  }
}
