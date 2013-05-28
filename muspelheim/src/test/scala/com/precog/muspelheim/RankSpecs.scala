package com.precog
package muspelheim

import com.precog.yggdrasil._

trait RankSpecs extends EvalStackSpecs {
  import stack._
  "Rank processing" should {
    "perform filter based on rank" >> {
      val input = """
        clicks := //clicks

        foo := solve 'userId
          clicks.time where clicks.userId = 'userId

        rank := std::stats::rank(foo)

        foo where rank >= 0
      """.stripMargin

      val input2 = """count((//clicks).time)"""
      val results2 = evalE(input2)
      val size = results2 collect { case (_, SDecimal(d)) => d.toInt }

      val result = evalE(input)

      val actual = result collect { case (ids, SDecimal(d)) if ids.size == 1 => d.toInt }
      val expected = evalE("(//clicks).time") collect { case (ids, SDecimal(d)) if ids.size == 1 => d.toInt }

      result must haveSize(size.head)
      actual mustEqual expected
    }

    "perform filter on distinct set based on rank with a solve" >> {
      val input = """
        clicks := //clicks

        foo := solve 'userId
          clicks.time where clicks.userId = 'userId

        distinctFoo := distinct(foo)

        rank := std::stats::rank(distinctFoo)

        distinctFoo where rank >= 0
      """.stripMargin

      val input2 = """count(distinct((//clicks).time))"""
      val results2 = evalE(input2)
      val size = results2 collect { case (_, SDecimal(d)) => d.toInt }

      val result = evalE(input)

      val actual = result collect { case (ids, SDecimal(d)) if ids.size == 1 => d.toInt }
      val expected = evalE("distinct((//clicks).time)") collect { case (ids, SDecimal(d)) if ids.size == 1 => d.toInt }

      result must haveSize(size.head)
      actual mustEqual expected
    }

    "perform another filter with rank" in {
      val input = """
        data := //summer_games/athletes 
        
        perCapitaAthletes := solve 'Countryname 
          data' := data where data.Countryname = 'Countryname
          {country: 'Countryname, 
            athletesPerMillion: count(data')/(data'.Population/1000000)} 
        
        distinctData := distinct(perCapitaAthletes) 
        
        rank := std::stats::rank(distinctData.athletesPerMillion) 
        distinctData with {rank: rank}
        """.stripMargin

      val result = evalE(input)
      val input2 = """
        data := //summer_games/athletes 
        
        perCapitaAthletes := solve 'Countryname 
          data' := data where data.Countryname = 'Countryname
          {country: 'Countryname, 
            athletesPerMillion: count(data')/(data'.Population/1000000)} 
        
        distinct(perCapitaAthletes)""".stripMargin

      val result2 = evalE(input2)
      val size = result2.size

      result must haveSize(size)

      result must haveAllElementsLike {
        case (ids, SObject(obj)) => {
          ids.length must_== 1
          obj must haveSize(3)
          obj must haveKey("rank")
          obj must haveKey("country")
          obj must haveKey("athletesPerMillion")
        }
      }
    }

    "perform filter on distinct set based on rank without a solve" >> {
      val input = """
        clicks := //clicks

        distinctFoo := distinct(clicks.time)

        rank := std::stats::rank(distinctFoo)

        distinctFoo where rank >= 0
      """.stripMargin

      val input2 = """count(distinct((//clicks).time))"""
      val results2 = evalE(input2)
      val size = results2 collect { case (_, SDecimal(d)) => d.toInt }

      val result = evalE(input)

      val actual = result collect { case (ids, SDecimal(d)) if ids.size == 1 => d.toInt }
      val expected = evalE("distinct((//clicks).time)") collect { case (ids, SDecimal(d)) if ids.size == 1 => d.toInt }

      result must haveSize(size.head)
      actual mustEqual expected
    }
   
    "perform filter on new set based on rank without a solve" >> {
      val input = """
        clicks := //clicks

        newFoo := new(clicks.time)

        rank := std::stats::rank(newFoo)

        newFoo where rank >= 0
      """.stripMargin

      val input2 = """count((//clicks).time)"""
      val results2 = evalE(input2)
      val size = results2 collect { case (_, SDecimal(d)) => d.toInt }

      val result = evalE(input)

      val actual = result collect { case (ids, SDecimal(d)) if ids.size == 1 => d.toInt }
      val expected = evalE("(//clicks).time") collect { case (ids, SDecimal(d)) if ids.size == 1 => d.toInt }

      result must not(beEmpty)
      result must haveSize(size.head)
      actual mustEqual expected
    }
    
    "ensure rows of rank 1 exist" >> {
      val input = """
        clicks := //clicks

        newFoo := new(clicks.time)

        rank := std::stats::rank(newFoo)

        newFoo where rank = 0
      """.stripMargin

      val input2 = """count(//clicks where (//clicks).time = min((//clicks).time))"""
      val results2 = evalE(input2)
      val size = results2 collect { case (_, SDecimal(d)) => d.toInt }

      val result = evalE(input)

      val actual = result collect { case (ids, SDecimal(d)) if ids.size == 1 => d.toInt }
      val expected = evalE("(//clicks).time where (//clicks).time = min((//clicks).time)") collect { case (ids, SDecimal(d)) if ids.size == 1 => d.toInt }

      result must haveSize(size.head)
      actual mustEqual expected
    }

    "evaluate rank" >> {
      "of the product of two sets" >> {
        val input = """
          | campaigns := //campaigns 
          | campaigns where std::stats::rank(campaigns.cpm * campaigns.cpm) = 36""".stripMargin

        val results = evalE(input) 
        
        results must haveSize(2)

        results must haveAllElementsLike {
          case (ids, SObject(obj)) => {
            ids.length must_== 1
            obj must haveSize(5)
            obj must contain("cpm" -> SDecimal(6))
          }
          case r => failure("Result has wrong shape: "+r)
        }
      }
      
      "using where" >> {
        val input = """
          | campaigns := //campaigns 
          | campaigns where std::stats::rank(campaigns.cpm) = 36""".stripMargin

        val results = evalE(input) 
        
        results must haveSize(2)

        results must haveAllElementsLike {
          case (ids, SObject(obj)) => {
            ids.length must_== 1
            obj must haveSize(5)
            obj must contain("cpm" -> SDecimal(6))
          }
          case r => failure("Result has wrong shape: "+r)
        }
      }

      "using where and with" >> {
        val input = """
          | campaigns := //campaigns
          | cpmRanked := campaigns with {rank: std::stats::rank(campaigns.cpm)}
          |   count(cpmRanked where cpmRanked.rank <= 37)""".stripMargin

        val results = eval(input)
        
        results mustEqual Set(SDecimal(38))
      }

      "using a solve" >> {
        val input = """
          | import std::stats::denseRank
          |
          | campaigns := //campaigns
          |
          | histogram := solve 'cpm
          |  {count: count(campaigns.cpm where campaigns.cpm = 'cpm), age: 'cpm}
          |
          | histogram with {rank: std::stats::rank(neg histogram.count)}""".stripMargin

        val results = eval(input)

        results must not be empty
      }
      
      "on a set of strings" >> {
        val input = """
          | std::stats::rank((//campaigns).campaign)""".stripMargin

        val results = eval(input) 
        
        val sanity = """
          | (//campaigns).campaign""".stripMargin

        val sanityCheck = eval(sanity)

        results must not be empty

        sanityCheck must not be empty
      }

      "using a solve more than once" >> {
        val input = """
          | medals := //summer_games/london_medals
          |
          | f(y, z) := solve 'age
          |   medals' := medals where y = 'age & z = "F"
          |   sum(medals'.Weight)
          |
          | {
          |   min: min(f(medals.Age, medals.Sex)),
          |   max: max(f(medals.Age, medals.Sex))
          | }
          | """.stripMargin

        val results = eval(input)

        results must not be empty
      }
    }

    "evaluate denseRank" >> {
      "using where" >> {
        val input = """
          | campaigns := //campaigns 
          | campaigns where std::stats::denseRank(campaigns.cpm) = 3""".stripMargin

        val results = evalE(input) 
        
        results must haveSize(2)

        results must haveAllElementsLike {
          case (ids, SObject(obj)) => {
            ids.length must_== 1
            obj must haveSize(5)
            obj must contain("cpm" -> SDecimal(6))
          }
          case r => failure("Result has wrong shape: "+r)
        }
      }

      "using where and with" >> {
        val input = """
          | campaigns := //campaigns
          | cpmRanked := campaigns with {rank: std::stats::denseRank(campaigns.cpm)}
          |   count(cpmRanked where cpmRanked.rank <= 4)""".stripMargin

        val results = eval(input) 
        
        results mustEqual Set(SDecimal(39))
      }
      
      "on a set of strings" >> {
        val input = """
          | std::stats::denseRank((//campaigns).campaign)""".stripMargin

        val results = eval(input) 
        
        val sanity = """
          | (//campaigns).campaign""".stripMargin

        val sanityCheck = eval(sanity)

        results must not be empty

        sanityCheck must not be empty
      }
    }

    "complete nathan's denseRank solve example" in {
      val input = """
        | import std::time::*
        | import std::stats::*
        | 
        | agents := //se/widget
        | 
        | upperBound := getMillis("2012-10-04T23:59:59")
        | lowerBound := getMillis("2012-10-04T00:00:00")
        | 
        | data := {agentId: agents.agentId, timeStamp: agents.timeStamp, action: agents.action, millis: getMillis(agents.timeStamp)}
        | 
        | getEvents(agent) := 
        |   data' := data where data.millis <= upperBound & data.millis >= lowerBound & data.agentId = agent
        | 
        |   data'' := data' with {rank: denseRank(data'.millis)}
        |   data''' := new data''
        | 
        |   result := solve 'rank
        |     r0 := data'' where data''.rank = 'rank
        |     r1 := data''' where data'''.rank = 'rank - 1
        | 
        |     r0 ~ r1
        |     {first: r0, second: r1}
        | 
        |   {start: result.first.millis, end: result.second.millis, agent: result.first.agentId, action: result.first.action} 
        | 
        | getEvents("Blake")
        | """.stripMargin
        
      evalE(input) must not(beEmpty)
    }

    "handle another case of solving on an object with denseRank" in {
      val input = """
        | import std::stats::*
        | 
        | upperBound := 1354122459346
        | lowerBound := 1354036059346
        | extraLB := lowerBound - (24*60*60000)
        | 
        | status := load("/8504352d-b063-400b-a10b-d6c637539469/status")
        | status' := status where status.timestamp <= upperBound & status.timestamp >= extraLB
        | 
        | results := solve 'agent
        |   data' := status' where status'.agentId = 'agent 
        |   
        |   rankedData := data' with {rank: denseRank(data'.timestamp)}
        | 
        |   result := solve 'rank
        |     first  := rankedData where rankedData.rank = 'rank
        |     second  := rankedData where rankedData.rank = 'rank + 1
        |     {first: first, second: second}
        | 
        |   {start: std::math::maxOf(result.first.timestamp, lowerBound), 
        |    end: result.second.timestamp, 
        |    agentId: result.first.agentId, 
        |    status: result.first.status, 
        |    name: result.first.agentAlias, 
        |    note: result.first.note }
        | 
        | results where results.end > lowerBound
        | """.stripMargin
        
      evalE(input) must not(throwAn[Exception])
    }
  }
}
