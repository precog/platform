package com.precog
package ragnarok
package test

object MiscStackSpecsSuite extends PerfTestSuite {
  "the full stack" := {
    "return count of empty set as 0 in body of solve" := {
      query("""
        | data := new {a: "down", b: 13}
        |
        | solve 'b
        |   xyz := data where data.b = 'b
        |   count(xyz where xyz.a = "up")
      """.stripMargin)
    }

    "return count of empty set as 0 in body of solve" := {
      query("""
        | data := new {a: "down", b: 13}
        |
        | solve 'b
        |   count(data where data.b = 'b & data.a = "up")
      """.stripMargin)
    }

    "join arrays after a relate" := {
      query("""
        medals' := //summer_games/london_medals
        medals'' := new medals'
  
        medals'' ~ medals'
        [medals'.Name, medals''.Name] where medals'.Name = medals''.Name""")
    }

    "join arrays with a nested operate, after a relate" := {
      query("""
        medals' := //summer_games/london_medals
        medals'' := new medals'
  
        medals'' ~ medals'
        [std::math::sqrt(medals''.Age), std::math::sqrt(medals'.Age)] where medals'.Age = medals''.Age""")
    }

    "ensure that we can call to `new` multiple times in a function and get fresh ids" := {
      query("""
        | f(x) := new x
        |
        | five := f(5)
        | six := f(6)
        | 
        | five ~ six
        |   five + six
      """.stripMargin)
    }

    "ensure that we can join after a join with the RHS" := {
      query("""
        | medals := //summer_games/london_medals
        | five := new 5 
        |
        | five ~ medals
        |   fivePlus := five + medals.Weight
        |   { weight: medals.Weight, increasedWeight: fivePlus }
        | """.stripMargin)
    }

    "ensure that we can join after a join with the LHS" := {
      query("""
        | medals := //summer_games/london_medals
        | five := new 5 
        |
        | five ~ medals
        |   fivePlus := five + medals.Weight
        |   { five: five, increasedWeight: fivePlus }
        | """.stripMargin)
    }

    "ensure that two array elements are not switched in a solve" := {
      query("""
        | orders := //orders
        | orders' := orders with { rank: std::stats::rank(orders.total) }
        |  
        | buckets := solve 'rank = orders'.rank
        |   minimum:= orders'.total where orders'.rank = 'rank
        |   maximum:= min(orders'.total where orders'.rank > 'rank)
        |   [minimum, maximum]
        | 
        | buckets
        | """.stripMargin)
    }

    "ensure that more than two array elements are not scrambled in a solve" := {
      query("""
        | orders := //orders
        | orders' := orders with { rank: std::stats::rank(orders.total) }
        |  
        | minimum:= orders'.total where orders'.rank = 1
        | maximum:= min(orders'.total where orders'.rank > 1)
        | [minimum, maximum, minimum, maximum]
        | """.stripMargin)
    }

    "ensure that with operation uses inner-join semantics" := {
      query("""
        | clicks := //clicks
        | a := {dummy: if clicks.time < 1329326691939 then 1 else 0}
        | clicks with {a:a}
        | """.stripMargin)
    }

    "filter set based on DateTime comparison using minTimeOf" := {
      query("""
        | clicks := //clicks
        | clicks' := clicks with { ISODateTime: std::time::parseDateTimeFuzzy(clicks.timeString) } 
        | 
        | minTime := std::time::minTimeOf("2012-02-09T00:31:13.610-09:00", clicks'.ISODateTime)
        |
        | clicks'.ISODateTime where clicks'.ISODateTime <= minTime
        | """.stripMargin)
    }

    "filter set based on DateTime comparison using reduction" := {
      query("""
        | clicks := //clicks
        | clicks' := clicks with { ISODateTime: std::time::parseDateTimeFuzzy(clicks.timeString) } 
        | 
        | minTime := minTime(clicks'.ISODateTime)
        |
        | clicks'.ISODateTime where clicks'.ISODateTime <= minTime
        | """.stripMargin)
    }

    "return a DateTime to the user as an ISO8601 String" := {
      query("""
        | clicks := //clicks
        | std::time::parseDateTimeFuzzy(clicks.timeString)
        | """.stripMargin)
    }

    "return a range of DateTime" := {
      query("""
        | clicks := //clicks
        |
        | start := std::time::parseDateTimeFuzzy(clicks.timeString)
        | end := std::time::yearsPlus(start, 2)
        | step := std::time::parsePeriod("P01Y")
        |
        | input := { start: start, end: end, step: step }
        |
        | std::time::range(input)
        | """.stripMargin)
    }

    "reduce sets" := {
      query("""
        | medals := //summer_games/london_medals
        |   sum(medals.HeightIncm) + mean(medals.Weight) - count(medals.Age) + stdDev(medals.S)
      """.stripMargin)
    }

    "recognize the datetime parse function" := {
      query("""
        | std::time::parseDateTime("2011-02-21 01:09:59", "yyyy-MM-dd HH:mm:ss")
      """.stripMargin)
    }

    "recognize and respect isNumber" := {
      query( """
        | london := //summer_games/london_medals
        | u := london.Weight union london.Country
        | u where std::type::isNumber(u)
      """.stripMargin)
    }

    "timelib functions should accept ISO8601 with a space instead of a T" := {
      query("""
        | std::time::year("2011-02-21 01:09:59")
      """.stripMargin)
    }

    "return the left size of a true if/else operation" := {
      query( """
        | if true then //clicks else //campaigns
      """.stripMargin)
    }

    "return the right size of a false if/else operation" := {
      query( """
        | if false then //clicks else //campaigns
      """.stripMargin)
    }

    "accept division inside an object" := {
      query("""
        | data := //conversions
        | 
        | x := solve 'productID
        |   data' := data where data.product.ID = 'productID
        |   { count: count(data' where data'.customer.isCasualGamer = false),
        |     sum: sum(data'.marketing.uniqueVisitors  where data'.customer.isCasualGamer = false) }
        | 
        | { max: max(x.sum/x.count), min: min(x.sum/x.count) }
      """.stripMargin)
    }

    "accept division of two BigDecimals" := {
      query("92233720368547758073 / 12223372036854775807")
    }

    "call the same function multiple times with different input" := {
      query("""
        | medals := //summer_games/london_medals
        | 
        | stats(variable) := {max: max(variable), min: min(variable), sum: sum(variable)}
        | 
        | weightStats := stats(medals.Weight)
        | heightStats := stats(medals.HeightIncm)
        | 
        | [weightStats, heightStats]
      """.stripMargin)
    }

    "perform various reductions on transspecable sets" := {
      query("""
        | medals := //summer_games/london_medals
        | 
        | { sum: sum(std::math::floor(std::math::cbrt(medals.HeightIncm))),
        |   max: max(medals.HeightIncm),
        |   min: min(medals.Weight),
        |   stdDev: stdDev(std::math::sqrt(medals.Weight)),
        |   count: count(medals.Weight = 39),
        |   minmax: min(max(medals.HeightIncm))
        | }
      """.stripMargin)
    }

    "solve on a union with a `with` clause" := {
      query("""
        | medals := //summer_games/london_medals
        | athletes := //summer_games/athletes
        | 
        | data := athletes union (medals with { winner: medals."Medal winner" })
        | 
        | solve 'winner 
        |   { winner: 'winner, num: count(data.winner where data.winner = 'winner) } 
      """.stripMargin)
    }

    "solve with a generic where inside a function" := {
      query("""
        | medals := //summer_games/london_medals
        | athletes := //summer_games/athletes
        | 
        | data := athletes union (medals with { winner: medals."Medal winner" })
        | 
        | f(x, y) := x where y
        | 
        | solve 'winner 
        |   { winner: 'winner, num: count(f(data.winner, data.winner = 'winner)) } 
      """.stripMargin)
    }
    
    "solve the results of a set and a stdlib op1 function" := {
      query("""
        | clicks := //clicks
        | clicks' := clicks with { foo: std::time::getMillis("2012-10-29") }
        | solve 'a clicks' where clicks'.time = 'a
        | """.stripMargin)
    }
    
    "solve involving extras with a stdlib op1 function" := {
      query("""
        | import std::time::*
        | 
        | agents := //clicks
        | data := { agentId: agents.userId, millis: getMillis(agents.timeString) }
        | 
        | upperBound := getMillis("2012-04-03T23:59:59")
        | 
        | solve 'agent
        |   data where data.millis < upperBound & data.agentId = 'agent
        | """.stripMargin)
    }

    "perform a simple join by value sorting" := {
      query("""
        | clicks := //clicks
        | views := //views
        |
        | clicks ~ views
        |   std::string::concat(clicks.pageId, views.pageId) where clicks.userId = views.userId
        """.stripMargin)
    }

    "union sets coming out of a solve" := {
      query("""
        clicks := //clicks
        foobar := solve 'a {userId: 'a, size: count(clicks where clicks.userId = 'a)}
        foobaz := solve 'b {pageId: 'b, size: count(clicks where clicks.pageId = 'b)}
        foobar union foobaz
      """.stripMargin)
    }

    "accept a solve involving a tic-var as an actual" := {
      query("""
        | medals := //summer_games/london_medals
        | 
        | age(x) := medals.Age = x
        |
        | x := solve 'age
        |   medals' := medals where age('age)
        |   sum(medals'.Weight where medals'.Sex = "F")
        | 
        | { min: min(x), max: max(x) }
      """.stripMargin)
    }

    "accept a solve involving a formal in a where clause" := {
      query("""
        | medals := //summer_games/london_medals
        | 
        | f(y) := solve 'age
        |   medals' := medals where y = 'age
        |   sum(medals'.Weight where medals'.Sex = "F")
        | 
        | { min: min(f(medals.Age)), max: max(f(medals.Age)) }
      """.stripMargin)
    }

    // Regression test for #39652091
    "call union on two dispatches of the same function" := {
      query("""
        | medals := //summer_games/london_medals
        |
        | f(x) :=
        |   medals' := medals where medals.Country = x
        |   medals'' := new medals'
        |
        |   medals'' ~ medals'
        |     {a: medals'.Country, b: medals''.Country} where medals'.Total = medals''.Total
        |
        | f("India") union f("Canada")
      """.stripMargin)
    }

    "return result for nested filters" := {
      query("""
        | medals := //summer_games/london_medals
        |
        | medals' := medals where medals.Country = "India"
        | medals'' := new medals'
        |
        | medals'' ~ medals'
        |   {a: medals'.Country, b: medals''.Country} where medals'.Total = medals''.Total
      """.stripMargin)
    }

    "accept a solve involving formals of formals" := {
      query("""
        | medals := //summer_games/london_medals
        | 
        | f(y) := 
        |   g(x) := 
        |     solve 'age
        |       medals' := medals where x = 'age
        |       sum(medals'.Weight where medals'.Sex = "F")
        |   g(y.Age)
        | 
        | { min: min(f(medals)), max: max(f(medals)) }
      """.stripMargin)
    }

    "correctly assign reductions to the correct field in an object" := {
      query("""
        | medals := //summer_games/london_medals
        |
        | x := solve 'age
        |   medals' := medals where medals.Age = 'age
        |   sum(medals'.Weight where medals'.Sex = "F")
        | 
        | { min: min(x), max: max(x) }
      """.stripMargin)
    }

    "correctly assign reductions to the correct field in an object with three reductions each on the same set" := {
      query("""
        | medals := //summer_games/london_medals
        |
        | x := solve 'age
        |   medals' := medals where medals.Age = 'age
        |   sum(medals'.Weight where medals'.Sex = "F")
        | 
        | { min: min(x), max: max(x), stdDev: stdDev(x) }
      """.stripMargin)
    }

    "correctly assign reductions to the correct field in an object with three reductions each on the same set" := {
      query("""
        | medals := //summer_games/london_medals
        |
        | x := solve 'age
        |   medals' := medals where medals.Age = 'age
        |   sum(medals'.Weight where medals'.Sex = "F")
        | 
        | { min: min(x), max: max(x), stdDev: stdDev(x) }
      """.stripMargin)
    }

    "accept a solve involving a where as an actual" := {
      query("""
        | clicks := //clicks
        | f(x) := x
        | counts := solve 'time
        |   {count: count(f(clicks where clicks.time = 'time)) }

        | cov := std::stats::cov(counts.count, counts.count)
        | counts with {covariance: cov}
        | """.stripMargin)
    }

    "accept a solve involving relation as an actual" := {
      query("""
        | clicks := //clicks
        | f(x) := x
        | counts := solve 'time
        |   { count: count(clicks where f(clicks.time = 'time)) }

        | cov := std::stats::cov(counts.count, counts.count)
        | counts with {covariance: cov}
        | """.stripMargin)
    }

    "accept covariance inside an object with'd with another object" := {
      query("""
        clicks := //clicks
        counts := solve 'time
          { count: count(clicks where clicks.time = 'time) }

        cov := std::stats::cov(counts.count, counts.count)
        counts with {covariance: cov}
      """.stripMargin)
    }
    
    "have the correct number of identities and values in a relate" := {
      "with the sum plus the LHS" := {
        query("""
          | //clicks ~ //campaigns
          | sum := (//clicks).time + (//campaigns).cpm
          | sum + (//clicks).time""".stripMargin)
      }
      
      "with the sum plus the RHS" := {
        query("""
          | //clicks ~ //campaigns
          | sum := (//clicks).time + (//campaigns).cpm
          | sum + (//campaigns).cpm""".stripMargin)
      }
    }
    
    "union two wheres of the same dynamic provenance" := {
      query("""
      | clicks := //clicks
      | clicks' := new clicks
      |
      | xs := clicks where clicks.time > 0
      | ys := clicks' where clicks'.pageId != "blah"
      |
      | xs union ys""".stripMargin)
    }

    "use the where operator on a unioned set" := {
      "campaigns.gender" := {
        query("""
          | a := //campaigns union //clicks
          |   a where a.gender = "female" """.stripMargin)
      }

      "clicks.platform" := {
        query("""
          | a := //campaigns union //clicks
          |   a where a.platform = "android" """.stripMargin)
      }
    }

    "basic set difference queries" := {
      "clicks difference clicks" := {
        query("//clicks difference //clicks")
      }
      "clicks.timeString difference clicks.timeString" := {
        query("(//clicks).timeString difference (//clicks).timeString")
      }      
    }

    "basic intersect and union queries" := {
      "constant intersection" := {
        query("4 intersect 4")
      }
      "constant union" := {
        query("4 union 5")
      }
      "empty intersection" := {
        query("4 intersect 5")
      }
      "heterogeneous union" := {
        query("{foo: 3} union 9")
      }
      "heterogeneous intersection" := {
        query("obj := {foo: 5} obj.foo intersect 5")
      }
      "intersection of differently sized arrays" := {
        query("arr := [1,2,3] arr[0] intersect 1")
      }
      "heterogeneous union doing strange things with identities" := {
        query("{foo: (//clicks).pageId, bar: (//clicks).userId} union //views")
      }
      "union with operation against same coproduct" := {
        query("(//clicks union //views).time + (//clicks union //views).time")
      }
      "union with operation on left part of coproduct" := {
        query("(//clicks union //views).time + (//clicks).time")
      }
      "union with operation on right part of coproduct" := {
        query("(//clicks union //views).time + (//views).time")
      }
    }

    "intersect a union" := {
      "campaigns.gender" := {
        query("""
          | campaign := (//campaigns).campaign
          | cpm := (//campaigns).cpm
          | a := campaign union cpm
          |   a intersect campaign """.stripMargin)
      }

      "union the same set when two different variables are assigned to it" := {
          query("""
            | a := //clicks
            | b := //clicks
            | a union b""".stripMargin)
      }

      "clicks.platform" := {
        query("""
          | campaign := (//campaigns).campaign
          | cpm := (//campaigns).cpm
          | a := campaign union cpm
          |   a intersect cpm """.stripMargin)
      }
    }

    "union with an object" := {
      query("""
        campaigns := //campaigns
        clicks := //clicks
        obj := {foo: campaigns.cpm, bar: campaigns.campaign}
        obj union clicks""".stripMargin)
    }

    "use the where operator on a key with string values" := {
      query("""//campaigns where (//campaigns).platform = "android" """)
    }

    "use the where operator on a key with numeric values" := {
      query("//campaigns where (//campaigns).cpm = 1 ")
    }

    "use the where operator on a key with array values" := {
      query("//campaigns where (//campaigns).ageRange = [37, 48]")
    }

    "evaluate the with operator across the campaigns dataset" := {
      query("count(//campaigns with { t: 42 })")
    }

    "perform distinct" := {
      "on a homogenous set of numbers" := {
        query("""
          | a := //campaigns
          |   distinct(a.gender)""".stripMargin)
      }

      "on set of strings formed by a union" := {
        query("""
          | gender := (//campaigns).gender
          | pageId := (//clicks).pageId
          | distinct(gender union pageId)""".stripMargin)
      }
    }

    "map object creation over the campaigns dataset" := {
      query("{ aa: (//campaigns).campaign }")
    }
    
    "perform a naive cartesian product on the campaigns dataset" := {
      query("""
        | a := //campaigns
        | b := new a
        |
        | a ~ b
        |   { aa: a.campaign, bb: b.campaign }""".stripMargin)
    }

    "correctly handle cross-match situations" := {
      query("""
        | campaigns := //campaigns
        | clicks := //clicks
        | 
        | campaigns ~ clicks
        |   campaigns = campaigns
        |     & clicks = clicks
        |     & clicks = clicks""".stripMargin)
    }

    "add sets of different types" := {
      "a set of numbers and a set of strings" := {
        query("(//campaigns).cpm + (//campaigns).gender")
      }

      "a set of numbers and a set of arrays" := {
        query("(//campaigns).cpm + (//campaigns).ageRange")
      }

      "a set of arrays and a set of strings" := {
        query("(//campaigns).gender + (//campaigns).ageRange")
      }
    }

    "return all possible value results from an underconstrained solve" := {
      query("""
        | campaigns := //campaigns
        | solve 'a 
        |   campaigns.gender where campaigns.platform = 'a""".stripMargin)
    }
    
    "determine a histogram of genders on campaigns" := {
      query("""
        | campaigns := //campaigns
        | solve 'gender 
        |   { gender: 'gender, num: count(campaigns.gender where campaigns.gender = 'gender) }""".stripMargin)
    }
    
    "determine a histogram of STATE on (tweets union tweets)" := {
      query("""
        | tweets := //election/tweets 
        | 
        | data := tweets union tweets
        | 
        | solve 'state 
        |   data' := data where data.STATE = 'state 
        |   {
        |     state: 'state, 
        |     count: count(data')
        |   }
        | """.stripMargin)
    }
    
    "evaluate nathan's query, once and for all" := {
      query("""
        | import std::time::*
        | 
        | lastHour := //election/tweets 
        | thisHour:= //election/tweets2 
        | 
        | lastHour' := lastHour where minuteOfHour(lastHour.timeStamp) > 36
        | data := thisHour union lastHour'
        | 
        | combined := solve 'stateName, 'state 
        |   data' := data where data.stateName = 'stateName & data.STATE = 'state 
        |   {stateName: 'stateName,
        |    state: 'state, 
        |    obamaSentimentScore: sum(data'.score where data'.candidate = "Obama") 
        |                         / count(data' where data'.candidate = "Obama"), 
        |    romneySentimentScore: sum(data'.score where data'.candidate = "Romney") 
        |                         / count(data' where data'.candidate = "Romney")} 
        | 
        | {stateName: combined.stateName, state: combined.state, sentiment: (50 * (combined.obamaSentimentScore - combined.romneySentimentScore)) + 50}
        | """.stripMargin)
    }

    "load a nonexistent dataset with a dot in the name" := {
      query("""
        | (//foo).bar""".stripMargin)
    }

    "deref an array with a where" := {
      query("""
        | a := [3,4,5]
        | a where a[0] = 1""".stripMargin)
    }

    "deref an object with a where" := {
      query("""
        | a := {foo: 5}
        | a where a.foo = 1""".stripMargin)
    }

    "evaluate reductions on filters" := {
      query("""
        | medals := //summer_games/london_medals
        | 
        |   {
        |   sum: sum(medals.Age where medals.Age = 30),
        |   mean: mean(std::math::maxOf(medals.B, medals.Age)),
        |   max: max(medals.G where medals.Sex = "F"),
        |   stdDev: stdDev(std::math::pow(medals.Total, medals.S))
        |   }
        """.stripMargin)
    }

    "evaluate single reduction on a filter" := {
      query("""
        | medals := //summer_games/london_medals
        | 
        | max(medals.G where medals.Sex = "F")
        """.stripMargin)
    }

    "evaluate single reduction on a object deref" := {
      query("""
        | medals := //summer_games/london_medals
        | 
        | max(medals.G)
        """.stripMargin)
    }

    "evaluate functions from each library" := {
      "Stringlib" := {
        query("""
          | gender := distinct((//campaigns).gender)
          | std::string::concat("alpha ", gender)""".stripMargin)
      }

      "Mathlib" := {
        query("""
          | cpm := distinct((//campaigns).cpm)
          | selectCpm := cpm where cpm < 10
          | std::math::pow(selectCpm, 2)""".stripMargin)
      }

      "Timelib" := {
        query("""
          | time := (//clicks).timeString
          | std::time::yearsBetween(time, "2012-02-09T19:31:13.616+10:00")""".stripMargin)
      }

      "Statslib" := {  //note: there are no identities because these functions involve reductions
        "Correlation" := {
          query("""
            | cpm := (//campaigns).cpm
            | std::stats::corr(cpm, 10)""".stripMargin)
        }

        // From bug #38535135
        "Correlation on solve results" := {
          query("""
            data := //summer_games/london_medals 
            byCountry := solve 'Country
              data' := data where data.Country = 'Country
              {country: 'Country,
              gold: sum(data'.G ),
              silver: sum(data'.S )}

            std::stats::corr(byCountry.gold,byCountry.silver)
            """)
        }

        "Covariance" := {
          query("""
            | cpm := (//campaigns).cpm
            | std::stats::cov(cpm, 10)""".stripMargin)
        }

        "Linear Regression" := {
          query("""
            | cpm := (//campaigns).cpm
            | std::stats::linReg(cpm, 10)""".stripMargin)
        }
      }
    }
 
    "set critical conditions given an empty set" := {
      query("""
        | solve 'a
        |   //campaigns where (//campaigns).foo = 'a""".stripMargin)
    }

    "use NotEq correctly" := {
      query("""//campaigns where (//campaigns).gender != "female" """.stripMargin)
    }

    "evaluate a solve constrained by inclusion" := {
      query("""
        | clicks := //clicks
        | views := //views
        |
        | solve 'page = views.pageId
        |   count(clicks where clicks.pageId = 'page)
        | """.stripMargin)
    }

    "evaluate sliding window in a" := {
      "solve expression" := {
        query("""
          | campaigns := //campaigns
          | nums := distinct(campaigns.cpm where campaigns.cpm < 10)
          | solve 'n
          |   m := max(nums where nums < 'n)
          |   (nums where nums = 'n) + m""".stripMargin)
      }
    }

    "evaluate a function of two parameters" := {
      query("""
        | fun(a, b) := 
        |   //campaigns where (//campaigns).ageRange = a & (//campaigns).gender = b
        | fun([25,36], "female")""".stripMargin)
    }

    "evaluate a solve of two parameters" := {
      query("""
        | campaigns := //campaigns
        | gender := campaigns.gender
        | platform := campaigns.platform
        | solve 'a, 'b
        |   g := gender where gender = 'a
        |   p := platform where platform = 'b
        |   campaigns where g = p""".stripMargin)
    }

    "determine a histogram of a composite key of revenue and campaign" := {
      query("""
        | campaigns := //campaigns
        | organizations := //organizations
        | 
        | solve 'revenue = organizations.revenue & 'campaign = organizations.campaign
        |   campaigns' := campaigns where campaigns.campaign = 'campaign
        |   { revenue: 'revenue, num: count(campaigns') }""".stripMargin)
    }

    "evaluate a function of multiple counts" := {
      query("""
        | import std::math::floor
        | clicks := //clicks
        | 
        | solve 'timeZone
        |   page0 := count(clicks.pageId where clicks.pageId = "page-0" & clicks.timeZone = 'timeZone)
        |   page1 := count(clicks.pageId where clicks.pageId = "page-1" & clicks.timeZone = 'timeZone)
        |   
        |   { timeZone: 'timeZone, ratio: floor(100 * (page0 / page1)) }
        """.stripMargin)
    }

    "evaluate reductions inside and outside of solves" := {
      query("""
        | clicks := //clicks
        |
        | countsForTimezone := solve 'timeZone
        |   clicksForZone := clicks where clicks.timeZone = 'timeZone
        |   {timeZone: 'timeZone, clickCount: count(clicksForZone)}
        |
        | mostClicks := max(countsForTimezone.clickCount)
        |
        | countsForTimezone where countsForTimezone.clickCount = mostClicks
        """.stripMargin)
    }

    "determine click times around each click" := {
      query("""
        | clicks := //clicks
        | 
        | solve 'time = clicks.time
        |   belowTime := max(clicks.time where clicks.time < 'time)
        |   aboveTime := min(clicks.time where clicks.time > 'time)
        |   
        |   {
        |     time: 'time,
        |     below: belowTime,
        |     above: aboveTime
        |   }
        """.stripMargin)
    }
     
  
    "determine most isolated clicks in time" := {
      query("""
        | clicks := //clicks
        | 
        | spacings := solve 'time
        |   click := clicks where clicks.time = 'time
        |   belowTime := max(clicks.time where clicks.time < 'time)
        |   aboveTime := min(clicks.time where clicks.time > 'time)
        |   
        |   {
        |     click: click,
        |     below: click.time - belowTime,
        |     above: aboveTime - click.time
        |   }
        | 
        | meanAbove := mean(spacings.above)
        | meanBelow := mean(spacings.below)
        | 
        | spacings.click where spacings.below > meanBelow & spacings.above > meanAbove""".stripMargin)
    }

    // Regression test for #39590007
    "give empty results when relation body uses non-existant field" := {
      query("""
        | clicks := //clicks
        | newClicks := new clicks
        |
        | clicks ~ newClicks
        |   {timeString: clicks.timeString, nonexistant: clicks.nonexistant}""".stripMargin)
    }
    
    "not explode on a large query" := {
      query("""
        | import std::stats::*
        | import std::time::*
        | 
        | agents := //se/status
        | bin5 := //bins/5
        | allBins := bin5.bin -5
        | 
        | minuteOfDay(time) := (hourOfDay(millisToISO(time, "+00:00")) * 60) + minuteOfHour(millisToISO(time, "+00:00"))
        | 
        | data' := agents with { minuteOfDay: minuteOfDay(agents.timestamp)}
        | 
        | upperBound := getMillis("2012-11-19T23:59:59")
        | lowerBound := getMillis("2012-11-19T00:00:00")
        | extraLB := lowerBound - (60*24*60000)
        | 
        | results := solve 'agent
        |   data'' := data' where data'.timestamp <= upperBound & data'.timestamp >= extraLB & data'.agentId = 'agent
        | 
        |   order := denseRank(data''.timestamp)
        |   data''' := data'' with {rank: order}
        | 
        |   newData := new data'''
        |   newData' := newData with {rank: newData.rank -1}
        | 
        |   result := newData' ~ data''
        | 
        |   {first: data''', second: newData'} where newData'.rank = data'''.rank
        |  
        |   {end: result.second.timestamp, 
        |   status: result.first.status, 
        |   startMinute: result.first.minuteOfDay, 
        |   endMinute: result.second.minuteOfDay}  
        | 
        | results' := results where results.end > lowerBound & results.status = "online"
        | 
        | solve 'bin = allBins.bin
        |   {bin: 'bin, count: count(results'.startMinute where results'.startMinute <= 'bin & results'.endMinute >= 'bin)}
        | """.stripMargin)
    }
    
    "solve on a constraint clause defined by an object with two non-const fields" := {
      query("""
        | clicks := //clicks
        | data := { user: clicks.user, page: clicks.page }
        | 
        | solve 'bins = data
        |   'bins
        | """.stripMargin)
    }
    
    "solve a chaining of user-defined functions involving repeated where clauses" := {
      query("""
        | import std::time::*
        | import std::stats::*
        | 
        | agents := load("/snapEngage/agents")
        | 
        | upperBound := getMillis("2012-04-03T23:59:59")
        | lowerBound := getMillis("2012-04-03T00:00:00")
        | extraLowerBound := lowerBound - (upperBound - lowerBound)/3 
        | 
        | data := {agentId: agents.agentId, timeStamp: agents.timeStamp, action: agents.action, millis: getMillis(agents.timeStamp)}
        | data' := data where data.millis < upperBound & data.millis > extraLowerBound & data.agentId = "agent1"
        | 
        | 
        | lastEvent(data) := data where data.millis = max(data.millis )
        | --lastEvent(data')
        | 
        | previousEvents(data, millis) := data where data.millis < millis
        | --previous := previousEvents(data', 1333477670000)
        | --last := lastEvent(previous)
        | --last
        | 
        | solve 'time = data'.millis
        |  {end: 'time, start: lastEvent(previousEvents(data', 'time))}
        | """.stripMargin)
    }
    
    "evaluate a trivial inclusion filter" := {
      query("""
        | t1 := //clicks
        | t2 := //views
        | 
        | t1 ~ t2
        |   t1 where t1.userId = t2.userId
        | """.stripMargin)
    }
    
    "handle a non-trivial solve on an object concat" := {
      query("""
        | agents := //se/widget
        | 
        | solve 'rank
        |   { agentId: agents.agentId } where { agentId: agents.agentId } = 'rank - 1
        | """.stripMargin)
    }

    "handle array creation with constant dispatch" := {
      query("""
        | a := 31
        |
        | error := 0.05
        | range(data) := [data*(1-error), data*(1 +error)]
        | range(a)
        | """.stripMargin)
    }

    "handle object creation with constant dispatch" := {
      query("""
        | a := 31
        |
        | error := 0.05
        | range(data) := {low: data*(1-error), high: data*(1 +error)}
        | range(a)
        | """.stripMargin)
    }
     
    "return the non-empty set for a trivial cartesian" := {
      query("""
        | jobs := //cm
        | titles' := new "foo"
        | 
        | titles' ~ jobs
        |   [titles', jobs.PositionHeader.PositionTitle]
        | """.stripMargin)
    }

    "produce a non-doubled result when counting the union of new sets" := {
      query("""
        | clicks := //clicks
        | clicks' := new clicks
        |
        | count(clicks' union clicks')
        | """.stripMargin)
    }

    "produce a non-doubled result when counting the union of new sets and a single set" := {
      query("""
        | clicks := //clicks
        | clicks' := new clicks
        |
        | [count(clicks' union clicks'), count(clicks')]
        | """.stripMargin)
    }

    "parse numbers correctly" := {
      query("""
        | std::string::parseNum("123")
        | """.stripMargin)
    }

    "toString numbers correctly" := {
      query("""
        | std::string::numToString(123)
        | """.stripMargin)
    }

    "correctly evaluate tautology on a filtered set" := {
      query("""
        | medals := //summer_games/london_medals
        | 
        | medals' := medals where medals.Country = "India"
        | medals'.Total = medals'.Total
        | """.stripMargin)
    }

    "produce a non-empty set for a dereferenced join-optimized cartesian" := {
      query("""
        | clicks := //clicks
        | clicks' := new clicks
        |
        | clicks ~ clicks'
        |   { a: clicks, b: clicks' }.a where [clicks'.pageId] = [clicks.pageId]
        | """.stripMargin)
    }

    "produce a non-empty set for a ternary join-optimized cartesian" := {
      query("""
        | clicks := //clicks
        | clicks' := new clicks
        |
        | clicks ~ clicks'
        |   { a: clicks, b: clicks', c: clicks } where clicks'.pageId = clicks.pageId
        | """.stripMargin)
    }

    "not produce out-of-order identities for simple cartesian and join with a reduction" := {
      query("""
        athletes := load("/summer_games/athletes")
        medals := load("/summer_games/london_medals")

        medals~athletes
        medalsWithPopulation := medals with {population: athletes.Population}
        results := medalsWithPopulation where athletes.Countryname = medals.Country
        count(results where results.Country = "Argentina")
        """)
    }
    
    "concatenate object projections on medals with inner-join semantics" := {
      query("""
        | medals := //summer_games/london_medals
        | { height: medals.HeightIncm, weight: medals.Weight }
        | """.stripMargin)
    }

    "work when tic-variable and reduction results are inlined" := {
      query("""
        | clicks := //clicks
        | solve 'c
        |   {c: 'c, n: count(clicks where clicks = 'c)}
        | """.stripMargin)
    }

    // Regression test for PLATFORM-951
    "evaluate SnapEngage query with code caught by predicate pullups" := {
      query("""
        | import std::stats::*
        | import std::time::*
        | data := //se/anon_status
        |
        | upperBound := 1353145306278
        | lowerBound := 1353135306278
        | extraLB := lowerBound - (24*60*60000)
        |
        | solve 'data
        |   data' := data where data.time <= upperBound & data.time >= extraLB & data.a = 'data
        |   order := denseRank(data'.time)
        |
        |   data'' := data' with { rank: order }
        |   newData := new data''
        |   newData' := newData with { rank: newData.rank - 1 }
        |
        |   result := newData' ~ data'' [ data'', newData' ] where newData'.rank = data''.rank
        |
        |   {start: std::math::max(result[0].time, lowerBound),
        |   end: result[1].time,
        |   a: result[0].a,
        |   b: result[0].b,
        |   c: result[0].c,
        |   data: result[0]}
        | """.stripMargin)
    }

    "use string function on date columns" := {
      query("""
        | import std::string::*
        | indexOf((//clicks).timeString, "-")
        | """.stripMargin)
    }
    
    "correctly filter the results of a non-trivial solve" := {
      query("""
        | import std::time::*
        | import std::stats::*
        | import std::string::*
        | 
        | contains(set, string) := indexOf(set, string) >= 0
        | 
        | allAccounts := //byAccount
        | accounts := allAccounts where !(contains(allAccounts.email, "test")) & !(contains(allAccounts.email, "precog.com")) & !(contains(allAccounts.email, "precog.io")) & !(contains(allAccounts.email, "reportgrid.com"))  & allAccounts.usage > 0
        | 
        | accounts' := accounts with {millis: getMillis(accounts.timestamp)}
        | accounts'' := accounts' with {rank: denseRank(accounts'.millis)}
        | 
        | r := solve 'email
        |   byEmail := accounts'' where accounts''.email = 'email
        |   
        |   today := byEmail where byEmail.rank = max(byEmail.rank)
        |   firstDay := byEmail where byEmail.rank = min(byEmail.rank)
        |   lastWeek := byEmail where byEmail.rank = max(byEmail.rank) - 7
        |   past30days := byEmail where byEmail.rank = max(byEmail.rank) - 30
        | 
        |   {
        |     email: 'email,
        |     accountCreatedOn: firstDay.timestamp,
        |     server: firstDay.server,
        |     account: firstDay.account,
        |     type: "Current Usage",
        |     value: sum(today.usage union (new 0) )
        | 
        |   }
        |   union
        |   {
        |     email: 'email,
        |     accountCreatedOn: firstDay.timestamp,
        |     server: firstDay.server,
        |     account: firstDay.account,
        |     type: "Increase Past Week",
        |     value:sum(lastWeek.usage union (new 0)) 
        |   }
        |   union
        |   {
        |     email: 'email,
        |     accountCreatedOn: firstDay.timestamp,
        |     server: firstDay.server,
        |     account: firstDay.account,
        |     type: "Increase Past 30 Days",
        |     value: sum(past30days.usage union (new 0))
        |   }
        |   
        | r where r.value > 0
        | """.stripMargin)
    }

    "successfully complete a query with a lot of unions" := {
      query("""
        | import std::time::*
        | import std::stats::*
        | import std::string::*
        | 
        | contains(set, string) := indexOf(set, string) >= 0
        | 
        | allAccounts := //byAccount
        | accounts := allAccounts where !(contains(allAccounts.email, "test")) & !(contains(allAccounts.email, "precog.com")) & !(contains(allAccounts.email, "precog.io")) & !(contains(allAccounts.email, "reportgrid.com"))  & allAccounts.usage > 0
        | 
        | accounts' := accounts with {millis: getMillis(accounts.timestamp)}
        | accounts'' := accounts' with {rank: denseRank(accounts'.millis)}
        | 
        | solve 'email
        |   byEmail := accounts'' where accounts''.email = 'email
        |   
        |   today := byEmail where byEmail.rank = max(byEmail.rank)
        |   firstDay := byEmail where byEmail.rank = min(byEmail.rank)
        |   yesterday :=  byEmail where byEmail.rank = max(byEmail.rank) - 1
        |   lastWeek := byEmail where byEmail.rank = max(byEmail.rank) - 7
        |   past30days := byEmail where byEmail.rank = max(byEmail.rank) - 30
        | 
        |   {
        |     email: 'email,
        |     accountCreatedOn: firstDay.timestamp,
        |     server: firstDay.server,
        |     account: firstDay.account,
        |     type: "Current Usage",
        |     value: sum(today.usage union (new 0) )
        | 
        |   }
        |   union
        |   {
        |     email: 'email,
        |     accountCreatedOn: firstDay.timestamp,
        |     server: firstDay.server,
        |     account: firstDay.account,
        |     type: "Increase Past Week",
        |     value:sum(lastWeek.usage union (new 0)) 
        |   }
        |   union
        |   {
        |     email: 'email,
        |     accountCreatedOn: firstDay.timestamp,
        |     server: firstDay.server,
        |     account: firstDay.account,
        |     type: "Increase Past 30 Days",
        |     value: sum(past30days.usage union (new 0))
        |   }
        |   union
        |   {
        |     email: 'email,
        |     accountCreatedOn: firstDay.timestamp,
        |     server: firstDay.server,
        |     account: firstDay.account,
        |     type: "Increase Since Yesterday",
        |     value: sum(yesterday.usage union (new 0))
        |   }
        | """.stripMargin)
    }
    
    // regression test for PLATFORM-986
    "not explode on mysterious error" := {
      query("""
        | import std::random::*
        | 
        | buckets := //benchmark/buckets/1361210162753
        | test := //test
        | 
        | r := observe(test, uniform(38))
        | 
        | pred := test ~ buckets
        |   range := buckets.range
        | 
        |   low := r >= range[1]
        |   hi := r < range[0]
        | 
        |   test with {prediction: buckets.name where low & hi}
        | 
        | solve 'zone
        |   pred' := pred where pred.currentZone = 'zone
        | 
        |   {
        |     tp: count(pred'.prediction where pred'.prediction = pred'.currentZone),
        |     fp: count(pred'.prediction where pred'.prediction != pred'.currentZone)    
        |   }
        | """.stripMargin)
    }
    
    //"not explode weirdly" := {
    //  val input = """
    //    | import std::stats::*
    //    | import std::time::*
    //    | 
    //    | --locations := //devicelocations/2012/07/01
    //    | locations := //test
    //    | deviceTimes := [ locations.deviceId, locations.captureTimestamp ]
    //    | 
    //    | order := denseRank(deviceTimes)
    //    | 
    //    | locations' := locations with { rank : order }
    //    | --locations'
    //    | newLocations := new locations'
    //    | newLocations' := newLocations with { rank : newLocations.rank - 1 }
    //    | 
    //    | joined := newLocations' ~ locations'
    //    |   { first : locations', second : newLocations' } where locations'.rank
    //    | = newLocations'.rank
    //    | 
    //    | r := joined where joined.first.deviceId = joined.second.deviceId
    //    | 
    //    | r' := 
    //    |   {
    //    |   data: r.first,
    //    |   nextLocation: r.second.currentZone,
    //    |   dwellTime: getMillis(r.second.captureTimestamp) - getMillis(r.first.captureTimestamp)
    //    |   }
    //    | 
    //    | markov := solve 'location, 'nextLocation
    //    |   r'' := r' where r'.data.currentZone = 'location & r'.nextLocation = 'nextLocation
    //    |   total := count(r'.data.currentZone where r'.data.currentZone = 'location)
    //    |   {
    //    |   matrix: ['location, 'nextLocation],
    //    |   prob: count(r''.nextLocation)/total
    //    |   }
    //    | 
    //    | --markov
    //    | 
    //    | --function for creating cumulative probability distribution and formatting intervals
    //    | createModel(data) :=  
    //    |   predictedDistribution := data with {rank : indexedRank(data.prob) }
    //    | 
    //    |   cumProb := solve 'rank = predictedDistribution.rank
    //    |     {
    //    |     location: predictedDistribution.matrix where  predictedDistribution.rank = 'rank,
    //    |     prob: sum(predictedDistribution.prob where predictedDistribution.rank <= 'rank),
    //    |     rank: 'rank
    //    |     }
    //    | 
    //    |   buckets := solve 'rank = cumProb.rank
    //    |     minimum:= cumProb.prob where cumProb.rank = 'rank
    //    |     maximum:= min(cumProb.prob where cumProb.rank > 'rank)
    //    |     {
    //    |      range: [minimum, maximum],
    //    |      name: cumProb.matrix where cumProb.rank = 'rank
    //    |     }
    //    | buckets
    //    | --end createModel Function
    //    | 
    //    | markov' := createModel(markov)
    //    | markov'
    //    | """.stripMargin
    //    
    //  eval(input) must not(throwA[Throwable])
    //}
    
    "produce something other than the empty set for join of conditional results" := {
      query("""
        | clicks := //clicks
        | 
        | predicted := clicks with { female : 0 }
        | observed := clicks with { female : if clicks = null then 1 else 0 }
        | 
        | [predicted, observed]
        | """.stripMargin)
    }
    
    "produce non-empty results when defining a solve with a conditional in a constraint" := {
      query("""
        | data := //clicks
        | 
        | import std::string::*
        | 
        | solve 'minexp
        |   idx := indexOf(data.userId, "-")
        |   exp := if idx < 0 then data.userId else takeLeft(data.userId, idx)
        |   exptrim := trim(exp)
        |   data2 := data where exptrim = 'minexp
        | 
        |   { MinExperience: 'minexp, numjobs: count(data2) }
        | """.stripMargin)
    }
    
    "not explode on multiply-used solve" := {
      query("""
        | travlex := //clicks
        | 
        | summarize(data, n) := solve data = 'qualifier
        |   'qualifier + n
        | 
        | summarize(travlex, 1) union
        | summarize(travlex, 2)
        | """.stripMargin)
    }
    
    "reduce the size of a filtered flattened array" := {
      query("""
        | foo := flatten([{ a: 1, b: 2 }, { a: 3, b: 4 }])
        | foo where foo.a = 1
        | """.stripMargin)
    }
  }
}

