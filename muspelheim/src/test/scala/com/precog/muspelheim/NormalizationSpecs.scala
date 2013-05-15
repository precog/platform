package com.precog
package muspelheim

import com.precog.yggdrasil._

trait NormalizationSpecs extends EvalStackSpecs {
  def summaryHeight(obj: Map[String, SValue]) = {
    obj("count") must beLike { case SDecimal(d) =>
      d.toDouble mustEqual 993
    }
    obj("mean") must beLike { case SDecimal(d) =>
      d.toDouble mustEqual 176.4370594159114
    }
    obj("min") must beLike { case SDecimal(d) =>
      d.toDouble mustEqual 140
    }
    obj("max") must beLike { case SDecimal(d) =>
      d.toDouble mustEqual 208
    }
    obj("stdDev") must beLike { case SDecimal(d) =>
      d.toDouble mustEqual 11.56375193112367
    }
  }

  "find simple summary" in {
    val input = """
      | medals := //summer_games/london_medals
      | summary(medals.HeightIncm)
    """.stripMargin

    val result = evalE(input) 

    result must haveSize(1)
    
    result must haveAllElementsLike {
      case (ids, SObject(model)) => {
        ids must haveSize(0)
        model.keySet mustEqual Set("model1")

        val SObject(obj) = model("model1")

        summaryHeight(obj)
      }
      case _ => ko
    }
  }

  "return empty set when summary called on non-numeric data" in {
    val input = """
      | medals := //summer_games/london_medals
      | summary(medals.Sport)
    """.stripMargin

    evalE(input) must beEmpty
  }

  "find summary and another reduction on the same dataset" in {
    val input = """
      | medals := //summer_games/london_medals
      | height := medals.HeightIncm
      | summary(height).model1 with { sqVariance: variance(height) ^ 2 }
    """.stripMargin

    val result = evalE(input) 

    result must haveSize(1)
    
    result must haveAllElementsLike {
      case (ids, SObject(obj)) => {
        ids must haveSize(0)

        summaryHeight(obj)

        obj("sqVariance") must beLike { case SDecimal(d) =>
          d.toDouble mustEqual 17881.13433742673
        }
      }
      case _ => ko
    }
  }

  def makeObject(query: String): Map[String, SValue] = {
    val result = evalE(query) collect { case (_, SObject(values)) => values("model1") }
    val SObject(obj) = result.head
    obj
  }

  "find summary of arrays, objects, and values" in {
    val input = """
      | medals := //summer_games/london_medals
      | height := { height: medals.HeightIncm }
      | weight := [medals.Weight]
      | age := medals.Age
      | sport := medals.Sport
      | data := height union weight union age union sport
      | summary(data)
    """.stripMargin

    val weight = """
      | medals := //summer_games/london_medals
      | summary(medals.Weight)
    """.stripMargin

    val height = """
      | medals := //summer_games/london_medals
      | summary(medals.HeightIncm)
    """.stripMargin

    val age = """
      | medals := //summer_games/london_medals
      | summary(medals.Age)
    """.stripMargin

    val result = evalE(input) 

    val weightResult = makeObject(weight)
    val heightResult = makeObject(height)
    val ageResult = makeObject(age)

    result must haveSize(1)
    
    result must haveAllElementsLike {
      case (ids, SObject(models)) => {
        ids must haveSize(0)
        models.keySet mustEqual Set("model1", "model2", "model3")

        def testModels(sv: SValue) = sv must beLike {
          case SObject(model) if model.keySet == Set("height") => {
            val SObject(summary) = model("height")
            summary mustEqual heightResult
          }

          case SObject(summary) => 
            summary mustEqual ageResult

          case SArray(model) => {
            model must haveSize(1)
            val SObject(summary) = model(0)
            summary mustEqual weightResult
          }

          case _ => ko
        }

        testModels(models("model1"))
        testModels(models("model2"))
        testModels(models("model3"))

      }
    }
  }

  "find summary with object" in {
    val input = """
      | medals := //summer_games/london_medals
      | data := {height: [medals.HeightIncm], weight: medals.Weight, age: medals.Age}
      | summary(data)
    """.stripMargin

    val schema1 = """
      | medals := //summer_games/london_medals
      | result :=
      |   { 
      |     age: medals.Age where std::type::isNumber(medals.Age)
      |   }
      | summary(result)
    """.stripMargin

    val schema2 = """
      | medals := //summer_games/london_medals
      | result :=
      |   {
      |     age: medals.Age where std::type::isNumber(medals.Age),
      |     height: [medals.HeightIncm] where std::type::isNumber(medals.HeightIncm)
      |   }
      | summary(result)
    """.stripMargin

    val schema3 = """
      | medals := //summer_games/london_medals
      | result :=
      |   {
      |     age: medals.Age where std::type::isNumber(medals.Age),
      |     weight: medals.Weight where std::type::isNumber(medals.Weight)
      |   }
      | summary(result)
    """.stripMargin

    val schema4 = """
      | medals := //summer_games/london_medals
      | result :=
      |   {
      |     age: medals.Age where std::type::isNumber(medals.Age),
      |     height: [medals.HeightIncm] where std::type::isNumber(medals.HeightIncm),
      |     weight: medals.Weight where std::type::isNumber(medals.Weight)
      |   }
      | summary(result)
    """.stripMargin

    val result = evalE(input) 

    result must haveSize(1)

    def makeObject(query: String): Map[String, SValue] = {
      val result = evalE(query) collect { case (_, SObject(values)) => values("model1") }
      val SObject(obj) = result.head
      obj
    }

    val obj1 = makeObject(schema1)
    val obj2 = makeObject(schema2)
    val obj3 = makeObject(schema3)
    val obj4 = makeObject(schema4)

    def testModel(model: SValue) = {
      val SObject(obj) = model

      if (Set("age", "height", "weight").subsetOf(obj.keySet))
        obj mustEqual obj4
      else if (Set("age", "height").subsetOf(obj.keySet))
        obj mustEqual obj2
      else if (Set("age", "weight").subsetOf(obj.keySet))
        obj mustEqual obj3
      else if (Set("age").subsetOf(obj.keySet))
        obj mustEqual obj1
      else 
        ko
    }

    result must haveAllElementsLike {
      case (ids, SObject(models)) => {
        ids must haveSize(0)
        models.keySet mustEqual Set("model1", "model2", "model3", "model4")

        testModel(models("model1"))
        testModel(models("model2"))
        testModel(models("model3"))
        testModel(models("model4"))
      }

      case _ => ko
    }
  }

  "find summary with object (2)" in {
    val input = """
      | medals := //summer_games/london_medals
      | fields := { 
      |   HeightIncm: medals.HeightIncm where std::type::isNumber(medals.HeightIncm),
      |   Age: medals.Age where std::type::isNumber(medals.Age),
      |   S: medals.S where std::type::isNumber(medals.S)
      | }
      | summary(fields)
    """.stripMargin

    val expectedInput = """
      | medals := //summer_games/london_medals
      | fields := { 
      |   HeightIncm: medals.HeightIncm where std::type::isNumber(medals.HeightIncm),
      |   Age: medals.Age where std::type::isNumber(medals.Age),
      |   S: medals.S where std::type::isNumber(medals.S)
      | }
      |
      | meanHeight := mean(medals.HeightIncm)
      | stdDevHeight := stdDev(medals.HeightIncm)
      |
      | meanAge := mean(medals.Age)
      | stdDevAge := stdDev(medals.Age)
      |
      | meanS := mean(medals.S)
      | stdDevS := stdDev(medals.S)
      | 
      | {
      |   HeightIncm: { mean: meanHeight, stdDev: stdDevHeight },
      |   Age: { mean: meanAge, stdDev: stdDevAge },
      |   S: { mean: meanS, stdDev: stdDevS }
      | }
    """.stripMargin

    val result0 = evalE(input)
    val expected0 = evalE(expectedInput)

    val result = result0 collect { case (ids, value) if ids.size == 1 => value }
    val expected = expected0 collect { case (ids, value) if ids.size == 1 => value }

    result mustEqual expected
  }

  "normalize data in simple case" in {
    val input = """
      | medals := //summer_games/london_medals
      | height := { HeightIncm: medals.HeightIncm }
      | summary := summary(height)
      | std::stats::normalize(medals, summary.model1)
    """.stripMargin

    val expectedInput = """
      | medals := //summer_games/london_medals
      |
      | meanHeight := mean(medals.HeightIncm)
      | stdDevHeight := stdDev(medals.HeightIncm)
      |
      | { HeightIncm: (medals.HeightIncm - meanHeight) / stdDevHeight }
    """.stripMargin

    val result = evalE(input) 
    val expected = evalE(expectedInput) 

    val resultValues = result collect {
      case (ids, value) if ids.size == 1 => value
    }

    val expectedValues = expected collect {
      case (ids, value) if ids.size == 1 => value
    }

    resultValues mustEqual expectedValues
  }

  "return empty set when there exists fields in summary not present in data" in {
    val input = """
      | medals := //summer_games/london_medals
      | height := { HeightIncm: medals.HeightIncm }
      | summary := summary(height).model1 with { foobar: { mean: 12, stdDev: 18 }}
      | std::stats::normalize(medals, summary)
    """.stripMargin

    evalE(input) must beEmpty
  }

  "normalize data with multiple fields" in {
    val input = """
      | medals := //summer_games/london_medals
      | fields := { 
      |   HeightIncm: medals.HeightIncm where std::type::isNumber(medals.HeightIncm),
      |   Age: medals.Age where std::type::isNumber(medals.Age),
      |   S: medals.S where std::type::isNumber(medals.S)
      | }
      | summary := summary(fields)
      | std::stats::normalize(medals, summary.model1)
    """.stripMargin

    val expectedInput = """
      | medals := //summer_games/london_medals
      |
      | fields := { 
      |   HeightIncm: medals.HeightIncm where std::type::isNumber(medals.HeightIncm),
      |   Age: medals.Age where std::type::isNumber(medals.Age),
      |   S: medals.S where std::type::isNumber(medals.S)
      | }
      |
      | meanHeight := mean(fields.HeightIncm)
      | stdDevHeight := stdDev(fields.HeightIncm)
      |
      | meanAge := mean(fields.Age)
      | stdDevAge := stdDev(fields.Age)
      |
      | meanS := mean(fields.S)
      | stdDevS := stdDev(fields.S)
      |
      | { 
      |   HeightIncm: (medals.HeightIncm - meanHeight) / stdDevHeight,
      |   Age: (medals.Age - meanAge) / stdDevAge,
      |   S: (medals.S - meanS) / stdDevS
      | }
    """.stripMargin

    val result = evalE(input) 
    val expected = evalE(expectedInput) 

    expected.size mustEqual result.size

    val resultValues = result collect {
      case (ids, value) if ids.size == 1 => value
    }

    val expectedValues = expected collect {
      case (ids, value) if ids.size == 1 => value
    }

    resultValues mustEqual expectedValues
  }

  "join normalize data with original data" in {
    val input = """
      | medals := //summer_games/london_medals
      | height := { HeightIncm: medals.HeightIncm }
      | summary := summary(height)
      | normalized := std::stats::normalize(medals, summary.model1)
      | { norm: normalized } with medals
    """.stripMargin

    val expectedInput = """
      | medals := //summer_games/london_medals
      |
      | meanHeight := mean(medals.HeightIncm)
      | stdDevHeight := stdDev(medals.HeightIncm)
      |
      | { norm: { HeightIncm: (medals.HeightIncm - meanHeight) / stdDevHeight } } with medals
    """.stripMargin

    val result = evalE(input) 
    val expected = evalE(expectedInput) 

    val resultValues = result collect {
      case (ids, value) if ids.size == 1 => value
    }

    val expectedValues = expected collect {
      case (ids, value) if ids.size == 1 => value
    }

    resultValues mustEqual expectedValues
  }

  "return empty set when normalizing and denormalizing when mean or stdDev is missing" in {
    val inputNorm = """
      | medals := //summer_games/london_medals
      | height := { HeightIncm: medals.HeightIncm }
      | std::stats::normalize(medals, {HeightIncm: {stdDev: 18}})
    """.stripMargin

    val inputDenorm = """
      | medals := //summer_games/london_medals
      | height := { HeightIncm: medals.HeightIncm }
      | std::stats::denormalize(medals, {HeightIncm: {mean: 23}})
    """.stripMargin

    val resultNorm = evalE(inputNorm) 
    val resultDenorm = evalE(inputDenorm) 

    resultNorm must beEmpty
    resultDenorm must beEmpty
  }

  "return empty set when normalizing and denormalizing with non-present schema" in {
    val inputNorm = """
      | medals := //summer_games/london_medals
      | height := { HeightIncm: medals.HeightIncm }
      | std::stats::normalize(medals, {foobar: {mean: 23, stdDev: 18}})
    """.stripMargin

    val inputDenorm = """
      | medals := //summer_games/london_medals
      | height := { HeightIncm: medals.HeightIncm }
      | std::stats::denormalize(medals, {foobar: {mean: 23, stdDev: 18}})
    """.stripMargin

    val resultNorm = evalE(inputNorm) 
    val resultDenorm = evalE(inputDenorm) 

    resultNorm must beEmpty
    resultDenorm must beEmpty
  }

  "denormalize normalized data" in {
    val input = """
      | medals := //summer_games/london_medals
      | height := { HeightIncm: medals.HeightIncm }
      |
      | summary := summary(height)
      | normalized := std::stats::normalize(medals, summary.model1)
      |
      | std::stats::denormalize(normalized, summary.model1)
    """.stripMargin

    val expectedInput = """
      | medals := //summer_games/london_medals
      | { HeightIncm: medals.HeightIncm where std::type::isNumber(medals.HeightIncm) }
    """.stripMargin

    val result = evalE(input) 
    val expected = evalE(expectedInput) 

    val resultValues = result collect {
      case (ids, value) if ids.size == 1 => value
    }

    val expectedValues = expected collect {
      case (ids, value) if ids.size == 1 => value
    }

    resultValues mustEqual expectedValues
  }

  "denormalize normalized data and join with original data" in {
    val input = """
      | medals := //summer_games/london_medals
      | height := { HeightIncm: medals.HeightIncm }
      |
      | summary := summary(height)
      |
      | normalized := std::stats::normalize(medals, summary.model1)
      | denormalized := std::stats::denormalize(normalized, summary.model1)
      |
      | denormalized with { originalHeight: medals.HeightIncm }
    """.stripMargin

    val result = evalE(input) 

    result must haveAllElementsLike {
      case (ids, SObject(obj)) => {
        ids must haveSize(1)
        obj.keySet mustEqual Set("originalHeight", "HeightIncm")
        
        obj("originalHeight") mustEqual obj("HeightIncm")
      }

      case _ => ko
    }
  }

  "denormalize normalized data after clustering" in {
    val input = """
      | conversions := //conversions
      |
      | ageNum := conversions.customer.age where std::type::isNumber(conversions.customer.age)
      | incomeNum := conversions.customer.income where std::type::isNumber(conversions.customer.income)
      |
      | input := { age: ageNum, income: incomeNum } 
      | summary := summary(input).model1
      | normed := std::stats::normalize(input, summary)
      |
      | clustering := std::stats::kMedians(normed, 10)
      |
      | std::stats::denormalize(clustering, summary)
    """.stripMargin

    val result = evalE(input) 
    result must not beEmpty

    val clusterIds = (1 to 10) map { "cluster" + _.toString }

    def clusterSchema(obj: Map[String, SValue], clusterId: String): Set[String] = obj(clusterId) match {
      case SObject(ctr) => ctr.keySet
      case _ => sys.error("malformed SObject")
    }

    result must haveAllElementsLike {
      case (ids, SObject(elems)) =>
        ids must haveSize(0)
        elems.keySet mustEqual Set("model1")

        elems("model1") must beLike { case SObject(clusters) =>
          clusters must haveSize(10)
          clusters.keySet mustEqual clusterIds.toSet
          
          val checkClusters = clusterIds.forall { clusterId =>
            clusterSchema(clusters, clusterId) == Set("age", "income")
          }

          if (checkClusters) ok
          else ko
        }

      case _ => ko
    }
  }
}
