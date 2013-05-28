package com.precog
package muspelheim

import com.precog.yggdrasil._

trait LinearRegressionSpecs extends EvalStackSpecs {
  import stack._

  def handleCoeffs(obj: Map[String, SValue]) = {
    obj.keys mustEqual Set("estimate", "standardError")

    obj("estimate") must beLike { case SDecimal(_) => ok }
    obj("standardError") must beLike { case SDecimal(_) => ok }
  }

  def handleZero(obj: Map[String, SValue]) = {
    obj.keys mustEqual Set("estimate", "standardError")

    obj("estimate") mustEqual SDecimal(0)
    obj("standardError") mustEqual SDecimal(0)
  }

  def checkFields(fields: Map[String, SValue]) = {
    val requiredFields = Set("coefficients", "RSquared", "varianceCovarianceMatrix", "residualStandardError")
    fields.keySet mustEqual requiredFields

    val rSquared = fields("RSquared")
    val resStdErr = fields("residualStandardError")
    val varCovar = fields("varianceCovarianceMatrix")

    resStdErr must beLike { case SObject(obj) =>
      obj.keySet mustEqual Set("estimate", "degreesOfFreedom")
      obj("estimate") must beLike { case SDecimal(_) => ok }
      obj("degreesOfFreedom") must beLike { case SDecimal(_) => ok }
    }

    rSquared must beLike { case SDecimal(_) => ok }
    varCovar must beLike { case SArray(_) => ok }
  }

  "linear regression" should {
    "not produce an exception in a corner case" in {
      val input = """
        | clicks := //clicks20k
        | std::stats::linearRegression(clicks.customer.age, clicks.product.price)
      """.stripMargin

      val result = evalE(input)
      result must haveSize(1)

      result must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          val SObject(fields) = elems("model1")

          val SArray(arr) = fields("coefficients")
          val rSquared = fields("RSquared")

          arr(0) must beLike { case SObject(obj) => handleCoeffs(obj) }

          arr(1) must beLike { case SObject(obj) => handleCoeffs(obj) }

          rSquared must beLike { case SDecimal(_) => ok }
        }
      }
    }

    "handle a constant x-values" in {
      val input = """
        | medals := //summer_games/london_medals
        | medals' := medals with { const: 3 }
        | 
        | std::stats::linearRegression(medals'.Weight, medals'.const)
      """.stripMargin

      val result = evalE(input)
      result must haveSize(1)  

      result must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          val SObject(fields) = elems("model1")

          val SArray(arr) = fields("coefficients")
          arr(0) must beLike { case SObject(obj) => handleZero(obj) }
          arr(1) must beLike { case SObject(obj) => handleCoeffs(obj) }

          checkFields(fields)
        }
      }
    }

    "handle multiple indep vars, one of which is a constant" in {
      val input = """
        | medals := //summer_games/london_medals
        | medals' := medals where std::type::isNumber(medals.HeightIncm) & std::type::isNumber(medals.Weight)
        | medals'' := medals' with { const: 3 }
        |
        | data := { const: medals''.const, height: medals''.HeightIncm }
        | 
        | std::stats::linearRegression(medals''.Weight, data)
      """.stripMargin

      val input2 = """
        | medals := //summer_games/london_medals
        | medals' := medals where std::type::isNumber(medals.HeightIncm) & std::type::isNumber(medals.Weight)
        |
        | data := { height: medals'.HeightIncm }
        | 
        | std::stats::linearRegression(medals'.Weight, data)
      """.stripMargin

      val result = evalE(input)
      val result2 = evalE(input2)

      result must haveSize(1)
      result2 must haveSize(1)

      val res1Values = new Array[BigDecimal](7)
      val res2Values = new Array[BigDecimal](7)

      val res1VarCovar = new Array[Vector[SValue]](1)
      val res2VarCovar = new Array[Vector[SValue]](1)

      def testResult(resValues: Array[BigDecimal], resVarCovar: Array[Vector[SValue]], elems: Map[String, SValue], constant: Boolean) = {
        elems.keys mustEqual Set("model1")

        val SObject(fields) = elems("model1")

        val SArray(arr) = fields("coefficients")
        val rSquared = fields("RSquared")
        val resStdErr = fields("residualStandardError")
        val varCovar = fields("varianceCovarianceMatrix")

        arr(0) must beLike { case SObject(obj) => 
          obj.keys mustEqual {
            if (constant) Set("const", "height")
            else Set("height")
          }

          obj("height") must beLike {
            case SObject(height) => 
              height.keys mustEqual Set("estimate", "standardError")

              height("estimate") must beLike { case SDecimal(d) => resValues(0) = d; ok }
              height("standardError") must beLike { case SDecimal(d) => resValues(1) = d; ok }
          }
          if (constant) {
            obj("const") must beLike {
              case SObject(const) => 
                const.keys mustEqual Set("estimate", "standardError")

                const("estimate") mustEqual SDecimal(0)
                const("standardError") mustEqual SDecimal(0)
            }
          } else {
            ok
          }
        }

        arr(1) must beLike { case SObject(obj) =>
          obj.keys mustEqual Set("estimate", "standardError")

          obj("estimate") must beLike { case SDecimal(d) => resValues(2) = d; ok }
          obj("standardError") must beLike { case SDecimal(d) => resValues(3) = d; ok }
        }

        rSquared must beLike { case SDecimal(d) => resValues(4) = d; ok }
        resStdErr must beLike { case SObject(obj) =>
          obj.keySet mustEqual Set("estimate", "degreesOfFreedom")
          obj("estimate") must beLike { case SDecimal(d) => resValues(5) = d; ok }
          obj("degreesOfFreedom") must beLike { case SDecimal(d) => resValues(6) = d; ok }
        }
        varCovar must beLike { case SArray(arr) => resVarCovar(0) = arr; ok }
      }

      result must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(0)
          testResult(res1Values, res1VarCovar, elems, true)
        }
      }

      result2 must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(0)
          testResult(res2Values, res2VarCovar, elems, false)
        }
      }
      
      res1Values mustEqual res2Values
      res1VarCovar mustEqual res2VarCovar
    }

    "predict when nulls are present in the regression model" in {
      val input = """
        | medals := //summer_games/london_medals
        | medals' := medals where std::type::isNumber(medals.HeightIncm) & std::type::isNumber(medals.Weight)
        | medals'' := medals' with { const: 3 }
        |
        | data := { const: medals''.const, height: medals''.HeightIncm }
        | model := std::stats::linearRegression(medals''.Weight, data)
        | input := { const: 12, height: medals.HeightIncm + 4 }
        |
        | std::stats::predictLinear(input, model)
      """.stripMargin

      val input2 = """ 
        | medals := //summer_games/london_medals
        |
        | h := medals where std::type::isNumber(medals.HeightIncm)
        | count(h)
      """.stripMargin

      val result = evalE(input)
      val resultCount = evalE(input2)

      val count = resultCount.collectFirst { case (_, SDecimal(d)) => d.toInt }.get
      result must haveSize(count)

      result must haveAllElementsLike { case (ids, SObject(elems)) =>
        ids must haveSize(1)
        elems.keys mustEqual Set("model1")

        elems("model1") must beLike { case SObject(model) => testPredict(model) }
      }
    }

    "accept a constant dependent variable" in {
      val input = """
        | medals := //summer_games/london_medals
        | medals' := medals with { const: 3 }
        | 
        | std::stats::linearRegression(medals'.const, medals'.Weight)
      """.stripMargin

      val result = evalE(input)
      result must haveSize(1)  

      result must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          val SObject(fields) = elems("model1")

          val SArray(arr) = fields("coefficients")
          val rSquared = fields("RSquared")

          rSquared must beLike { case SDecimal(_) => ok }
        }
      }
    }

    "accept a previous problematic case regarding slice boundaries" in {
      val input = """
        | conversions := //conversions
        | conversions' := conversions with
        | 
        |  {
        |    female : if conversions.customer.gender = "female" then 1 else 0
        |  }
        |
        | independentVars := {
        |   income: conversions'.customer.income, 
        |   age: conversions'.customer.age,
        |   female: conversions'.female
        |   }
        | 
        | std::stats::linearRegression(conversions'.product.price, independentVars)
      """.stripMargin

      val result = evalE(input)
      result must haveSize(1)  

      result must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          val SObject(fields) = elems("model1")

          checkFields(fields)
        }
      }
    }

    "accept linearly dependent variables" in {
      val input = """
        | clicks := //clicks2
        |
        | vars := {
        |   rate: clicks.marketing.bounceRate,
        |   rate2: (clicks.marketing.bounceRate * 2) + 1
        | }
        | 
        | std::stats::linearRegression(clicks.product.price, vars)
      """.stripMargin

      val result = evalE(input)
      result must haveSize(1)  

      result must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          val SObject(fields) = elems("model1")

          val SArray(arr) = fields("coefficients")

          arr(0) must beLike { case SObject(obj) => 
            obj.keys mustEqual Set("rate", "rate2")

            obj("rate2") must beLike {
              case SObject(height) =>  handleCoeffs(height)
            }
            obj("rate") must beLike {
              case SObject(const) => handleZero(const)
            }
          }

          arr(1) must beLike { case SObject(obj) => handleCoeffs(obj) }

          checkFields(fields)
        }
      }
    }

    "accept linearly dependent variables in case with three columns" in {
      val input = """
        | clicks := //clicks2
        |
        | vars := {
        |   rate: clicks.marketing.bounceRate,
        |   rateprice: (clicks.marketing.bounceRate * 2) + (clicks.product.price * 3) + 5,
        |   price: clicks.product.price + 8
        | }
        | 
        | std::stats::linearRegression(clicks.product.price, vars)
      """.stripMargin

      val result = evalE(input)
      result must haveSize(1)  

      result must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          val SObject(fields) = elems("model1")

          val SArray(arr) = fields("coefficients")

          arr(0) must beLike { case SObject(obj) => 
            obj.keys mustEqual Set("rate", "rateprice", "price")

            obj("rate") must beLike {
              case SObject(rate) => handleCoeffs(rate)
            }
            obj("rateprice") must beLike {
              case SObject(rateprice) => handleCoeffs(rateprice)
            }
            obj("price") must beLike {
              case SObject(price) => handleZero(price)
            }
          }

          arr(1) must beLike { case SObject(obj) => handleCoeffs(obj) }

          checkFields(fields)
        }
      }
    }

    "accept linearly dependent variables in case with two dependent columns" in {
      val input = """
        | clicks := //clicks2
        |
        | vars := {
        |   rate: clicks.marketing.bounceRate,
        |   rateprice: (clicks.marketing.bounceRate * 2) + (clicks.product.price * 3) + 5,
        |   price: clicks.product.price + 8,
        |   zzz: clicks.product.price + 9
        | }
        | 
        | std::stats::linearRegression(clicks.product.price, vars)
      """.stripMargin

      val result = evalE(input)
      result must haveSize(1)  

      result must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          val SObject(fields) = elems("model1")

          val SArray(arr) = fields("coefficients")

          arr(0) must beLike { case SObject(obj) => 
            obj.keys mustEqual Set("rate", "rateprice", "price", "zzz")

            obj("price") must beLike {
              case SObject(price) => handleZero(price)
            }
            obj("rateprice") must beLike {
              case SObject(rateprice) => handleCoeffs(rateprice)
            }
            obj("zzz") must beLike {
              case SObject(zzz) => handleCoeffs(zzz)
            }
            obj("rate") must beLike {
              case SObject(rate) => handleZero(rate)
            }
          }

          arr(1) must beLike { case SObject(obj) => handleCoeffs(obj) }

          checkFields(fields)
        }
      }
    }

    "return correctly structured results in a simple case of linear regression" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | std::stats::linearRegression(medals.Weight, { height: medals.HeightIncm })
      """.stripMargin

      val results = evalE(input)
      results must haveSize(1)  

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          val SObject(fields) = elems("model1")

          val SArray(arr) = fields("coefficients")

          arr(0) must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("height")

            obj("height") must beLike {
              case SObject(height) => handleCoeffs(height)
            }
          }

          arr(1) must beLike { case SObject(obj) => handleCoeffs(obj) }

          checkFields(fields)
      }
    }

    "predict linear regression" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | model := std::stats::linearRegression(medals.Weight, { height: medals.HeightIncm })
        | std::stats::predictLinear({height: 34, other: 35}, model)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(1)  

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("model1")

          elems("model1") must beLike { case SObject(model) => testPredict(model) }
      }
    }

    def testPredict(model: Map[String, SValue]) = {
      model.keySet mustEqual Set("fit", "confidenceInterval", "predictionInterval")

      model("fit") must beLike { case SDecimal(_) => ok }
      model("confidenceInterval") must beLike { case SArray(arr) =>
        arr must haveSize(2)
        arr(0) must beLike { case SDecimal(_) => ok }
        arr(1) must beLike { case SDecimal(_) => ok }
      }
      model("predictionInterval") must beLike { case SArray(arr) =>
        arr must haveSize(2)
        arr(0) must beLike { case SDecimal(_) => ok }
        arr(1) must beLike { case SDecimal(_) => ok }
      }
    }

    def testJoinLinear(input: String, input2: String, idJoin: Boolean) = {
      val results = evalE(input)
      val resultsCount = evalE(input2)

      val count = resultsCount.collectFirst { case (_, SDecimal(d)) => d.toInt }.get
      results must haveSize(count)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          if (idJoin) ids must haveSize(2)
          else ids must haveSize(1)

          elems.keys must contain("predictedWeight")

          elems("predictedWeight") must beLike { case SObject(obj) =>
            obj.keySet mustEqual Set("model1")

            obj("model1") must beLike { case SObject(model) => testPredict(model) }
          }
        }
      }
    }

    "join predicted results with original dataset" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | model := std::stats::linearRegression(medals.Weight, { HeightIncm: medals.HeightIncm })
        | predictions := std::stats::predictLinear(medals, model)
        |
        | medals with { predictedWeight: predictions }
      """.stripMargin

      val input2 = """ 
        | medals := //summer_games/london_medals
        |
        | h := medals where std::type::isNumber(medals.HeightIncm)
        | count(h)
      """.stripMargin

      testJoinLinear(input, input2, false)
    }

    "join predicted results with original dataset when model is `new`ed" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | model := new std::stats::linearRegression(medals.Weight, { HeightIncm: medals.HeightIncm })
        |
        | model ~ medals
        | predictions := std::stats::predictLinear(medals, model)
        |
        | medals with { predictedWeight: predictions }
      """.stripMargin

      val input2 = """ 
        | medals := //summer_games/london_medals
        |
        | h := medals where std::type::isNumber(medals.HeightIncm)
        | count(h)
      """.stripMargin

      testJoinLinear(input, input2, true)
    }

    "join predicted results with model when model is `new`ed" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | model := new std::stats::linearRegression(medals.Weight, { HeightIncm: medals.HeightIncm })
        |
        | model ~ medals
        | predictions := std::stats::predictLinear(medals, model)
        |
        | model with { predictedWeight: predictions }
      """.stripMargin

      val input2 = """ 
        | medals := //summer_games/london_medals
        |
        | h := medals where std::type::isNumber(medals.HeightIncm)
        | count(h)
      """.stripMargin

      val results = evalE(input)
      val resultsCount = evalE(input2)

      val count = resultsCount.collectFirst { case (_, SDecimal(d)) => d.toInt }.get
      results must haveSize(count)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) => {
          ids must haveSize(2)
          elems.keys mustEqual Set("predictedWeight", "model1")

          elems("predictedWeight") must beLike { case SObject(obj) =>
            obj.keys mustEqual Set("model1")

            obj("model1") must beLike { case SObject(model) => testPredict(model) }
          }
        }
      }
    }

    "predict linear regression when no field names in model are present in data" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | model := std::stats::linearRegression(medals.Weight, { height: medals.HeightIncm })
        | std::stats::predictLinear({weight: 34, other: 35}, model)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(0)
    }

    "return correct number of results in more complex case of linear regression" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | std::stats::linearRegression(medals.S, medals)
      """.stripMargin

      val results = evalE(input)

      results must haveSize(1)

      results must haveAllElementsLike {
        case (ids, SObject(elems)) =>
          ids must haveSize(0)
          elems.keys mustEqual Set("model1", "model2", "model3", "model4")
      }
    }

    "throw exception when fed rank deficient data" in {
      val input = """
        std::stats::linearRegression(0, 4)
      """

      evalE(input) must throwA[IllegalArgumentException]
    }

    "return empty set when the dependent variable is not at the root path" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | std::stats::linearRegression({weight: medals.Weight}, {height: medals.HeightIncm})
      """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when given feature values of wrong type" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | std::stats::linearRegression(medals.WeightIncm, medals.Country)
      """.stripMargin

      evalE(input) must beEmpty
    }

    "return empty set when given dependent values of wrong type" in {
      val input = """
        | medals := //summer_games/london_medals
        | 
        | std::stats::linearRegression(medals.Country, medals.WeightIncm)
      """.stripMargin

      evalE(input) must beEmpty
    }

    "return precise regression results" in {
      val inputModel = """
        | data := //small
        | 
        | std::stats::linearRegression(data[1], data[0])
      """.stripMargin

      val inputPrediction = """
        | data := //small
        | 
        | model := std::stats::linearRegression(data[1], data[0])
        | std::stats::predictLinear(data[0], model)
      """.stripMargin

      val model = evalE(inputModel)
      val prediction = evalE(inputPrediction)

      model must haveSize(1)
      prediction must haveSize(25)  

      model must haveAllElementsLike { case (ids, values) =>  
        ids must haveSize(0)
        values mustEqual
          SObject(Map(
            "model1" -> SObject(Map(
              "residualStandardError" -> SObject(Map(
                "degreesOfFreedom" -> SDecimal(21),
                "estimate" -> SDecimal(0.5714938945329378))),
              "varianceCovarianceMatrix" -> SArray(Vector(
                SArray(Vector(SDecimal(0.029179094730367727), SDecimal(0.000005132290591506178), SDecimal(0.0012941789523089743), SDecimal(-4.641629704480576))), 
                SArray(Vector(SDecimal(0.000005132290591506234), SDecimal(1.7232175428295578E-7), SDecimal(0.0000026890467037135995), SDecimal(0.0014286230989684486))), 
                SArray(Vector(SDecimal(0.0012941789523089743), SDecimal(0.000002689046703713597), SDecimal(0.0015709703355553353), SDecimal(-0.2661999465513123))), 
                SArray(Vector(SDecimal(-4.641629704480575), SDecimal(0.0014286230989684614), SDecimal(-0.2661999465513122), SDecimal(1389.6506067743135))))),
              "coefficients" -> SArray(Vector(
                SObject(Map(
                  "x1" -> SObject(Map(
                    "estimate" -> SDecimal(-0.000353609260803943),
                    "standardError" -> SDecimal(0.0004151165550576799))),
                  "x2" -> SObject(Map(
                    "standardError" -> SDecimal(0.03963546815108074),
                    "estimate" -> SDecimal(0.058266092643983664))),
                  "x3" -> SObject(Map(
                    "standardError" -> SDecimal(37.2780177420194),
                    "estimate" -> SDecimal(-13.04059432806976))))),
                SObject(Map(
                  "estimate" -> SDecimal(0.06398145974561122),
                  "standardError" -> SDecimal(0.17081889453560964))))), 
              "RSquared" -> SDecimal(0.1389644021522881)))))
      }

      prediction must haveAllElementsLike { case (ids, _) =>
        ids must haveSize(1)
      }

      val predResults = prediction collect { case (_, values) => values }

      predResults mustEqual Set(
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(0.2033781409054441),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.29431560466544177), SDecimal(0.7010718864763299))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.0851091500695673), SDecimal(1.4918654318804554))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(-0.09168503550146807),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.5851022333835115), SDecimal(0.4017321623805754))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.3785265042883241), SDecimal(1.1951564332853881))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(0.41044957243898667),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.13780702485772428), SDecimal(0.9587061697356976))),
          "predictionInterval" -> SArray(Vector(SDecimal(-0.8983994376088162), SDecimal(1.7192985824867897))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(0.34350266219169356),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.15613475300076995), SDecimal(0.8431400773841571))),
          "predictionInterval" -> SArray(Vector(SDecimal(-0.9457366411511546), SDecimal(1.6327419655345419))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(0.3664477456323341),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.23055994092248494), SDecimal(0.9634554321871531))),
          "predictionInterval" -> SArray(Vector(SDecimal(-0.9635592515031257), SDecimal(1.6964547427677938))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(-0.29914967551765137),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.7742788311824955), SDecimal(0.17597948014719278))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.5790903574191737), SDecimal(0.9807910063838711))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(-0.022684748200555035),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.4898668510205224), SDecimal(0.4444973546194123))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.2996967104855732), SDecimal(1.2543272140844632))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(-0.23185255903279373),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.6846724956403343), SDecimal(0.22096737757474683))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.5036805071503851), SDecimal(1.0399753890847976))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(0.10434021762615728),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.16240567781368945), SDecimal(0.37108611306600403))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.1137130683009206), SDecimal(1.322393503553235))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(-0.12491148621376584),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.5735350403390753), SDecimal(0.3237120679115436))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.3952514123515578), SDecimal(1.145428439924026))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(0.07369191728264393),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.5673976301980359), SDecimal(0.7147814647633237))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.276677011015371), SDecimal(1.424060845580659))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(0.15981827006475147),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.32431880487926934), SDecimal(0.6439553450087723))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.1234935166648408), SDecimal(1.4431300567943437))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(-0.17753635730419182),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.6672157676701567), SDecimal(0.31214305306177303))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.4629492713361403), SDecimal(1.1078765567277564))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(-0.05314337388394709),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.4396759324088084), SDecimal(0.33338918464091427))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.302906492808298), SDecimal(1.196619745040404))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(-0.31383525200320217),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.7744700890718244), SDecimal(0.14679958506541996))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.588466527213795), SDecimal(0.9607960232073907))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(-0.24955772615782634),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.8258596856896088), SDecimal(0.3267442333739561))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.5704000268068927), SDecimal(1.07128457449124))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(0.28854308478343343),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.17900619990234334), SDecimal(0.7560923694692102))),
          "predictionInterval" -> SArray(Vector(SDecimal(-0.9886032530470941), SDecimal(1.5656894226139608))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(-0.21068040495425405),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.588435827141518), SDecimal(0.16707501723300994))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.4577568234690235), SDecimal(1.0363960135605153))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(-0.08002882783141475),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.543625794379431), SDecimal(0.38356813871660156))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.355733567187771), SDecimal(1.1956759115249416))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(0.16098562305018183),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.2120317372092274), SDecimal(0.5340029833095911))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.0846637600743971), SDecimal(1.4066350061747608))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(0.2163660943315413),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.12064440737692839), SDecimal(0.553376596040011))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.0189785996245788), SDecimal(1.4517107882876614))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(-0.009355951358066453),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.5430850388059062), SDecimal(0.5243731360897733))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.3121863982741837), SDecimal(1.2934744955580506))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(0.23267835643463947),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.17818883336490904), SDecimal(0.6435455462341879))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.0248240420829635), SDecimal(1.4901807549522426))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(0.023449692507401948),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.36960590484028055), SDecimal(0.4165052898550844))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.2283462721889578), SDecimal(1.2752456572037616))))))),
        SObject(Map("model1" -> SObject(Map(
          "fit" -> SDecimal(0.03949810192952638),
          "confidenceInterval" -> SArray(Vector(SDecimal(-0.52151233487641), SDecimal(0.6005085387354628))),
          "predictionInterval" -> SArray(Vector(SDecimal(-1.2747443204678535), SDecimal(1.3537405243269063))))))))
    }
  }
}
