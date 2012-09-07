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
import com.precog.common._
import org.specs2.mutable._

trait EvalStackSpecs extends Specification {
  def eval(str: String, debug: Boolean = false): Set[SValue]
  def evalE(str: String, debug: Boolean = false): Set[SEvent]

  "the full stack" should {
    "count a filtered clicks dataset" in {
      val input = """
        | clicks := //clicks
        | count(clicks where clicks.time > 0)""".stripMargin
        
      eval(input) mustEqual Set(SDecimal(100))
    }

    "count the campaigns dataset" >> {
      "<root>" >> {
        eval("count(//campaigns)") mustEqual Set(SDecimal(100))
      }
      
      "gender" >> {
        eval("count(//campaigns.gender)") mustEqual Set(SDecimal(100))
      }
      
      "platform" >> {
        eval("count(//campaigns.platform)") mustEqual Set(SDecimal(100))
      }
      
      "campaign" >> {
        eval("count(//campaigns.campaign)") mustEqual Set(SDecimal(100))
      }
      
      "cpm" >> {
        eval("count(//campaigns.cpm)") mustEqual Set(SDecimal(100))
      }

      "ageRange" >> {
        eval("count(//campaigns.ageRange)") mustEqual Set(SDecimal(100))
      }
    }

    "reduce the obnoxiously large dataset" >> {
      "<root>" >> {
        eval("mean(//obnoxious.v)", true) mustEqual Set(SDecimal(50000.5))
      }
    }

    "accept !true and !false" >> {
      "!true" >> {
        eval("!true") mustEqual Set(SBoolean(false))
      }

      "!false" >> {
        eval("!false") mustEqual Set(SBoolean(true))
      }
    }

    "accept a dereferenced array" >> {
      "non-empty array" >> {
        eval("[1,2,3].foo") mustEqual Set()
      }

      "empty array" >> {
        eval("[].foo") mustEqual Set()
      }.pendingUntilFixed
    }

    "accept a dereferenced object" >> {
      "non-empty object" >> {
        eval("{a: 42}[1]") mustEqual Set()
      }

      "empty object" >> {
        eval("{}[0]") mustEqual Set()
      }.pendingUntilFixed
    }    
    
    "accept a where'd empty array and empty object" >> {
      "empty object (left)" >> {
        eval("{} where true") mustEqual Set()
      }.pendingUntilFixed

      "empty object (right)" >> {
        eval("true where {}") mustEqual Set()
      }.pendingUntilFixed
      
      "empty array (left)" >> {
        eval("[] where true") mustEqual Set()
      }.pendingUntilFixed

      "empty array (right)" >> {
        eval("true where []") mustEqual Set()
      }.pendingUntilFixed
    }    
    
    "accept a with'd empty array and empty object" >> {
      "empty object (left)" >> {
        eval("{} with true") mustEqual Set()
      }.pendingUntilFixed

      "empty object (right)" >> {
        eval("true with {}") mustEqual Set()
      }.pendingUntilFixed
      
      "empty array (left)" >> {
        eval("[] with true") mustEqual Set()
      }.pendingUntilFixed

      "empty array (right)" >> {
        eval("true with []") mustEqual Set()
      }.pendingUntilFixed
    }    

    "union sets coming out of a forall" >> {
      val input = """
        clicks := //clicks
        foobar := forall 'a {userId: 'a, size: count(clicks where clicks.userId = 'a)}
        foobaz := forall 'b {pageId: 'b, size: count(clicks where clicks.pageId = 'b)}
        foobar union foobaz
      """.stripMargin

      val results = evalE(input)

      results must haveSize(10)

      forall(results) {
        case (ids, SObject(obj)) => {
          ids must haveSize(1)
          obj must haveSize(1)
          obj must haveKey("bar") or haveKey("baz")
        }
      }
    }.pendingUntilFixed
    
    "have the correct number of identities and values in a relate" >> {
      "with the sum plus the LHS" >> {
        val input = """
          | //clicks ~ //campaigns
          | sum := //clicks.time + //campaigns.cpm
          | sum + //clicks.time""".stripMargin

        val results = evalE(input)

        results must haveSize(10000)

        forall(results) {
          case (ids, _) => ids must haveSize(2)
        }
      }.pendingUntilFixed
      
      "with the sum plus the RHS" >> {
        val input = """
          | //clicks ~ //campaigns
          | sum := //clicks.time + //campaigns.cpm
          | sum + //campaigns.cpm""".stripMargin

        val results = evalE(input)

        results must haveSize(10000)

        forall(results) {
          case (ids, _) => ids must haveSize(2)
        }
      }.pendingUntilFixed
    }

    "union two wheres of the same dynamic provenance" >> {
      val input = """
      | clicks := //clicks
      | clicks' := new clicks
      |
      | xs := clicks where clicks.time > 0
      | ys := clicks' where clicks'.pageId != "blah"
      |
      | xs union ys""".stripMargin

      val results = evalE(input)

      results must haveSize(200)
    }.pendingUntilFixed

    "use the where operator on a unioned set" >> {
      "campaigns.gender" >> {
        val input = """
          | a := //campaigns union //clicks
          |   a where a.gender = "female" """.stripMargin
          
        val results = evalE(input)
        
        results must haveSize(46)
        
        forall(results) {
          case (VectorCase(_), SObject(obj)) => {
            obj must haveSize(5)
            obj must contain("gender" -> SString("female"))
          }
          case r => failure("Result has wrong shape: "+r)
        }
      }.pendingUntilFixed

      "clicks.platform" >> {
        val input = """
          | a := //campaigns union //clicks
          |   a where a.platform = "android" """.stripMargin
          
        val results = evalE(input)
        
        results must haveSize(72)
        
        forall(results) {
          case (VectorCase(_), SObject(obj)) => {
            obj must haveSize(5)
            obj must contain("platform" -> SString("android"))
          }
          case r => failure("Result has wrong shape: "+r)
        }
      }.pendingUntilFixed
    }

    "basic set difference queries" >> {
      {
        val input = "//clicks difference //campaigns"
        val results = evalE(input)

        results must haveSize(100)
      }
      {
        val input = "//clicks difference //clicks"
        val results = evalE(input)

        results must haveSize(0)
      }
      {
        val input = "//clicks difference //clicks.timeString"
        val results = evalE(input)

        results must haveSize(0)
      }      
      {
        val input = "//clicks.time difference //clicks.timeString"
        val results = evalE(input)

        results must haveSize(0)
      }
    }.pendingUntilFixed

    "basic intersect and union queries" >> {
      {
        val input = "4 intersect 4"
        val results = evalE(input)

        results must haveSize(1)
        
        forall(results) {
          case (VectorCase(_), SDecimal(d)) => { d mustEqual 4 }
          case r => failure("Result has wrong shape: "+r)
        }
      }
      {
        val input = "4 union 5"
        val results = evalE(input)

        results must haveSize(2)
        
        forall(results) {
          case (VectorCase(_), SDecimal(d)) => { Set(4,5) must contain(d) }
          case r => failure("Result has wrong shape: "+r)
        }
      }
      {
        val input = "//clicks intersect //views"
        val results = evalE(input)

        results must beEmpty
      }
      {
        val input = "{foo: 3} union 9"
        val results = evalE(input)

        results must haveSize(2)
        
        forall(results) {
          case (VectorCase(_), SDecimal(d)) => { d mustEqual 4 }
          case (VectorCase(_), SObject(obj)) => { obj must contain("foo" -> 3) }
          case r => failure("Result has wrong shape: "+r)
        }
      }
      {
        val input = "obj := {foo: 5} obj.foo intersect 5"
        val results = evalE(input)

        results must haveSize(1)
        
        forall(results) {
          case (VectorCase(_), SDecimal(d)) => { d mustEqual 5 }
          case r => failure("Result has wrong shape: "+r)
        }
      }
      {
        val input = "arr := [1,2,3] arr[0] intersect 1"
        val results = evalE(input)

        results must haveSize(1)
        
        forall(results) {
          case (VectorCase(_), SDecimal(d)) => { d mustEqual 1 }
          case r => failure("Result has wrong shape: "+r)
        }
      }
      {
        val input = "{foo: //clicks.pageId, bar: //clicks.userId} union //views"
        val results = evalE(input)

        results must haveSize(200)
      }
    }.pendingUntilFixed

    "intersect a union" >> {
      "campaigns.gender" >> {
        val input = """
          | campaign := //campaigns.campaign
          | cpm := //campaigns.cpm
          | a := campaign union cpm
          |   a intersect campaign """.stripMargin
          
        val results = evalE(input)
        
        results must haveSize(100)
        
        forall(results) {
          case (VectorCase(_), SString(campaign)) =>
            Set("c16","c9","c21","c15","c26","c5","c18","c7","c4","c17","c11","c13","c12","c28","c23","c14","c10","c19","c6","c24","c22","c20") must contain(campaign)
          case r => failure("Result has wrong shape: "+r)
        }
      }.pendingUntilFixed

      "union the same set when two different variables are assigned to it" >> {
          val input = """
            | a := //clicks
            | b := //clicks
            | a union b""".stripMargin

          val results = evalE(input)

          results must haveSize(100)
      }.pendingUntilFixed      

      "clicks.platform" >> {
        val input = """
          | campaign := //campaigns.campaign
          | cpm := //campaigns.cpm
          | a := campaign union cpm
          |   a intersect cpm """.stripMargin
          
        val results = evalE(input)
        
        results must haveSize(100)
        
        forall(results) {
          case (VectorCase(_), SDecimal(num)) => {
            Set(100,39,91,77,96,99,48,67,10,17,90,58,20,38,1,43,49,23,72,42,94,16,9,21,52,5,40,62,4,33,28,54,70,82,76,22,6,12,65,31,80,45,51,89,69) must contain(num)
          }
          case r => failure("Result has wrong shape: "+r)
        }
      }.pendingUntilFixed
    }

    "union with an object" >> {
      val input = """
        campaigns := //campaigns
        clicks := //clicks
        obj := {foo: campaigns.cpm, bar: campaigns.campaign}
        obj union clicks""".stripMargin

      val results = evalE(input)

      results must haveSize(200)
    }.pendingUntilFixed

    "use the where operator on a key with string values" in {
      val input = """//campaigns where //campaigns.platform = "android" """
      val results = evalE(input)
      
      results must haveSize(72)

      forall(results) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(5)
          obj must contain("platform" -> SString("android"))
        }
        case r => failure("Result has wrong shape: "+r)
      }
    }

    "use the where operator on a key with numeric values" in {
      val input = "//campaigns where //campaigns.cpm = 1 "
      val results = evalE(input)
      
      results must haveSize(34)

      forall(results) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(5)
          obj must contain("cpm" -> SDecimal(1))
        }
        case r => failure("Result has wrong shape: "+r)
      }
    }

    "use the where operator on a key with array values" in {
      val input = "//campaigns where //campaigns.ageRange = [37, 48]"
      val results = evalE(input)
      
      results must haveSize(39)

      forall(results) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(5)
          obj must contain("ageRange" -> SArray(Vector(SDecimal(37), SDecimal(48))))
        }
        case r => failure("Result has wrong shape: "+r)
      }
    }.pendingUntilFixed

    "evaluate the with operator across the campaigns dataset" in {
      val input = "count(//campaigns with { t: 42 })"
      eval(input) mustEqual Set(SDecimal(100))
    }

    "perform distinct" >> {
      "on a homogenous set of numbers" >> {
        val input = """
          | a := //campaigns
          |   distinct(a.gender)""".stripMargin

        eval(input) mustEqual Set(SString("female"), SString("male"))   
      }

      "on set of strings formed by a union" >> {
        val input = """
          | gender := //campaigns.gender
          | pageId := //clicks.pageId
          | distinct(gender union pageId)""".stripMargin

        eval(input) mustEqual Set(SString("female"), SString("male"), SString("page-0"), SString("page-1"), SString("page-2"), SString("page-3"), SString("page-4"))   
      }.pendingUntilFixed
    }

    "map object creation over the campaigns dataset" in {
      val input = "{ aa: //campaigns.campaign }"
      val results = evalE(input)
      
      results must haveSize(100)
      
      forall(results) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(1)
          obj must haveKey("aa")
        }
        case r => failure("Result has wrong shape: "+r)
      }
    }
    
    "perform a naive cartesian product on the campaigns dataset" in {
      val input = """
        | a := //campaigns
        | b := new a
        |
        | a ~ b
        |   { aa: a.campaign, bb: b.campaign }""".stripMargin
        
      val results = evalE(input)
      
      results must haveSize(10000)
      
      forall(results) {
        case (VectorCase(_, _), SObject(obj)) => {
          obj must haveSize(2)
          obj must haveKey("aa")
          obj must haveKey("bb")
        }
        case r => failure("Result has wrong shape: "+r)
      }
    }

    "correctly handle cross-match situations" in {
      val input = """
        | campaigns := //campaigns
        | clicks := //clicks
        | 
        | campaigns ~ clicks
        |   campaigns = campaigns
        |     & clicks = clicks
        |     & clicks = clicks""".stripMargin
        
      eval(input) must not(beEmpty)
    }.pendingUntilFixed    

    "add sets of different types" >> {
      "a set of numbers and a set of strings" >> {
        val input = "//campaigns.cpm + //campaigns.gender"

        eval(input) mustEqual Set()
      }

      "a set of numbers and a set of arrays" >> {
        val input = "//campaigns.cpm + //campaigns.ageRange"

        eval(input) mustEqual Set()
      }

      "a set of arrays and a set of strings" >> {
        val input = "//campaigns.gender + //campaigns.ageRange"

        eval(input) mustEqual Set()
      }
    }

    "return only all possible value results from a" >> {
      "characteristic function" >> {
        val input = """
          | campaigns := //campaigns
          | f('a) :=
          |   campaigns.gender where campaigns.platform = 'a
          | f""".stripMargin
          
        val results = evalE(input)
        
        results must haveSize(100)
        
        forall(results) {
          case (VectorCase(_), SString(gender)) =>
            gender must beOneOf("male", "female")
          case r => failure("Result has wrong shape: "+r)
        }
      }.pendingUntilFixed

      "forall expression" >> {
        val input = """
          | campaigns := //campaigns
          | forall 'a 
          |   campaigns.gender where campaigns.platform = 'a""".stripMargin
          
        val results = evalE(input)
        
        results must haveSize(100)
        
        forall(results) {
          case (VectorCase(_), SString(gender)) =>
            gender must beOneOf("male", "female")
          case r => failure("Result has wrong shape: "+r)
        }
      }.pendingUntilFixed
    }
    
    "determine a histogram of genders on campaigns" >> {
      "characteristic function" >> { 
        val input = """
          | campaigns := //campaigns
          | hist('gender) :=
          |   { gender: 'gender, num: count(campaigns.gender where campaigns.gender = 'gender) }
          | hist""".stripMargin
          
        eval(input) mustEqual Set(
          SObject(Map("gender" -> SString("female"), "num" -> SDecimal(46))),
          SObject(Map("gender" -> SString("male"), "num" -> SDecimal(54))))
      }.pendingUntilFixed

      "forall expression" >> { 
        val input = """
          | campaigns := //campaigns
          | forall 'gender 
          |   { gender: 'gender, num: count(campaigns.gender where campaigns.gender = 'gender) }""".stripMargin
          
        eval(input) mustEqual Set(
          SObject(Map("gender" -> SString("female"), "num" -> SDecimal(46))),
          SObject(Map("gender" -> SString("male"), "num" -> SDecimal(54))))
      }.pendingUntilFixed
    }

    "load a nonexistent dataset with a dot in the name" in {
      val input = """
        | //foo.bar""".stripMargin
     
      eval(input) mustEqual Set()
    }

    "deref an array with a where" in {
      val input = """
        | a := [3,4,5]
        | a where a[0] = 1""".stripMargin

      val results = eval(input)

      results must haveSize(0)
    }

    "deref an object with a where" in {
      val input = """
        | a := {foo: 5}
        | a where a.foo = 1""".stripMargin

      val results = eval(input)

      results must haveSize(0)
    }

    "evaluate rank" >> {
      "of the product of two sets" >> {
        val input = """
          | campaigns := //campaigns 
          | campaigns where std::stats::rank(campaigns.cpm * campaigns.cpm) = 37""".stripMargin

        val results = evalE(input) 
        
        results must haveSize(2)

        forall(results) {
          case (VectorCase(_), SObject(obj)) => {
            obj must haveSize(5)
            obj must contain("cpm" -> SDecimal(6))
          }
          case r => failure("Result has wrong shape: "+r)
        }
      }.pendingUntilFixed      
      
      "using where" >> {
        val input = """
          | campaigns := //campaigns 
          | campaigns where std::stats::rank(campaigns.cpm) = 37""".stripMargin

        val results = evalE(input) 
        
        results must haveSize(2)

        forall(results) {
          case (VectorCase(_), SObject(obj)) => {
            obj must haveSize(5)
            obj must contain("cpm" -> SDecimal(6))
          }
          case r => failure("Result has wrong shape: "+r)
        }
      }.pendingUntilFixed

      "using where and with" >> {
        val input = """
          | campaigns := //campaigns
          | cpmRanked := campaigns with {rank: std::stats::rank(campaigns.cpm)}
          |   count(cpmRanked where cpmRanked.rank <= 37)""".stripMargin

        val results = eval(input)
        
        results mustEqual Set(SDecimal(38))
      }.pendingUntilFixed      
      
      "on a set of strings" >> {
        val input = """
          | std::stats::rank(//campaigns.userId)""".stripMargin

        val results = eval(input) 
        
        val sanity = """
          | //campaigns.userId""".stripMargin

        val sanityCheck = eval(sanity)

        results must be empty

        sanityCheck must not be empty
      }.pendingUntilFixed
    }

    "evaluate denseRank" >> {
      "using where" >> {
        val input = """
          | campaigns := //campaigns 
          | campaigns where std::stats::denseRank(campaigns.cpm) = 4""".stripMargin

        val results = evalE(input) 
        
        results must haveSize(2)

        forall(results) {
          case (VectorCase(_), SObject(obj)) => {
            obj must haveSize(5)
            obj must contain("cpm" -> SDecimal(6))
          }
          case r => failure("Result has wrong shape: "+r)
        }
      }.pendingUntilFixed

      "using where and with" >> {
        val input = """
          | campaigns := //campaigns
          | cpmRanked := campaigns with {rank: std::stats::denseRank(campaigns.cpm)}
          |   count(cpmRanked where cpmRanked.rank <= 5)""".stripMargin

        val results = eval(input) 
        
        results mustEqual Set(SDecimal(39))
      }.pendingUntilFixed      
      
      "on a set of strings" >> {
        val input = """
          | std::stats::denseRank(//campaigns.userId)""".stripMargin

        val results = eval(input) 
        
        val sanity = """
          | //campaigns.userId""".stripMargin

        val sanityCheck = eval(sanity)

        results must be empty

        sanityCheck must not be empty
      }.pendingUntilFixed
    }

    "evaluate functions from each library" >> {
      "Stringlib" >> {
        val input = """
          | gender := distinct(//campaigns.gender)
          | std::string::concat("alpha ", gender)""".stripMargin

        eval(input) mustEqual Set(SString("alpha female"), SString("alpha male"))
      }

      "Mathlib" >> {
        val input = """
          | cpm := distinct(//campaigns.cpm)
          | selectCpm := cpm where cpm < 10
          | std::math::pow(selectCpm, 2)""".stripMargin

        eval(input) mustEqual Set(SDecimal(25), SDecimal(1), SDecimal(36), SDecimal(81), SDecimal(16))
      }.pendingUntilFixed

      "Timelib" >> {
        val input = """
          | time := //clicks.timeString
          | std::time::yearsBetween(time, "2012-02-09T19:31:13.616+10:00")""".stripMargin

        val results = evalE(input) 
        val results2 = results map {
          case (VectorCase(_), SDecimal(d)) => d.toInt
          case r => failure("Result has wrong shape: "+r)
        }

        results2 must contain(0).only
      }

      "Statslib" >> {  //note: there are no identities because these functions involve reductions
        "Correlation" >> {
          val input = """
            | cpm := //campaigns.cpm
            | std::stats::corr(cpm, 10)""".stripMargin

          val results = evalE(input) 
          val results2 = results map {
            case (VectorCase(), SDecimal(d)) => d.toDouble
            case r => failure("Result has wrong shape: "+r)
          }

          results2 must haveSize(0)
        }

        "Covariance" >> {
          val input = """
            | cpm := //campaigns.cpm
            | std::stats::cov(cpm, 10)""".stripMargin

          val results = evalE(input) 
          results must haveSize(1)

          val results2 = results map {
            case (VectorCase(), SDecimal(d)) => d.toDouble
            case r => failure("Result has wrong shape: "+r)
          }
          results2 must contain(0)
        }

        "Linear Regression" >> {
          val input = """
            | cpm := //campaigns.cpm
            | std::stats::linReg(cpm, 10)""".stripMargin

          val results = evalE(input) 
          results must haveSize(1)

          val results2 = results map {
            case (VectorCase(), SObject(fields)) => fields
            case r => failure("Result has wrong shape: "+r)
          }
          results2 must contain(Map("slope" -> SDecimal(0), "intercept" -> SDecimal(10)))
        }
      }
    }
 
    "set critical conditions given an empty set in" >> {
      "characteristic function" >> {
        val input = """
          | function('a) :=
          |   //campaigns where //campaigns.foo = 'a
          | function""".stripMargin

        eval(input) mustEqual Set()
      }.pendingUntilFixed

      "forall expression" >> {
        val input = """
          | forall 'a
          |   //campaigns where //campaigns.foo = 'a""".stripMargin

        eval(input) mustEqual Set()
      }.pendingUntilFixed
    }

    "use NotEq correctly" in {
      val input = """//campaigns where //campaigns.gender != "female" """.stripMargin

      val results = evalE(input)

      forall(results) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(5)
          obj must contain("gender" -> SString("male"))
        }
        case r => failure("Result has wrong shape: "+r)
      }
    }

    "evaluate sliding window in a" >> {
        "characteristic function" >> {
        val input = """
          | campaigns := //campaigns
          | nums := distinct(campaigns.cpm where campaigns.cpm < 10)
          | sums('n) :=
          |   m := max(nums where nums < 'n)
          |   (nums where nums = 'n) + m 
          | sums""".stripMargin

        eval(input) mustEqual Set(SDecimal(15), SDecimal(11), SDecimal(9), SDecimal(5))
      }.pendingUntilFixed

      "forall expression" >> {
        val input = """
          | campaigns := //campaigns
          | nums := distinct(campaigns.cpm where campaigns.cpm < 10)
          | forall 'n
          |   m := max(nums where nums < 'n)
          |   (nums where nums = 'n) + m""".stripMargin

        eval(input) mustEqual Set(SDecimal(15), SDecimal(11), SDecimal(9), SDecimal(5))
      }.pendingUntilFixed
    }

    "evaluate a quantified characteristic function of two parameters" in {
      val input = """
        | fun('a, 'b) := 
        |   //campaigns where //campaigns.ageRange = 'a & //campaigns.gender = 'b
        | fun([25,36], "female")""".stripMargin

      val results = evalE(input) 
      results must haveSize(14)
      
      forall(results) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(5)
          obj must contain("ageRange" -> SArray(Vector(SDecimal(25), SDecimal(36))))
          obj must contain("gender" -> SString("female"))
        }
        case r => failure("Result has wrong shape: "+r)
      }
    }.pendingUntilFixed

    "evaluate a function of two parameters" >> {  //note: this is NOT the the most efficient way to implement this query, but it still should work
      "characteristic function" >> {
        val input = """
          | campaigns := //campaigns
          | gender := campaigns.gender
          | platform := campaigns.platform
          | equality('a, 'b) :=
          |   g := gender where gender = 'a
          |   p := platform where platform = 'b
          |   campaigns where g = p
          | equality""".stripMargin

        eval(input) mustEqual Set()
      }.pendingUntilFixed

      "forall expression" >> {
        val input = """
          | campaigns := //campaigns
          | gender := campaigns.gender
          | platform := campaigns.platform
          | forall 'a forall 'b
          |   g := gender where gender = 'a
          |   p := platform where platform = 'b
          |   campaigns where g = p""".stripMargin

        eval(input) mustEqual Set()
      }.pendingUntilFixed
    }

    "determine a histogram of genders on category" in {
      val input = """
        | campaigns := //campaigns
        | organizations := //organizations
        | 
        | hist('revenue, 'campaign) :=
        |   organizations' := organizations where organizations.revenue = 'revenue
        |   campaigns' := campaigns where campaigns.campaign = 'campaign
        |   organizations'' := organizations' where organizations'.campaign = 'campaign
        |   
        |   campaigns' ~ organizations''
        |     { revenue: 'revenue, num: count(campaigns') }
        |   
        | hist""".stripMargin

      todo //eval(input) mustEqual Set()   
    }.pendingUntilFixed
     
    "determine most isolated clicks in time" >> {
      "characteristic function" >> {
        val input = """
          | clicks := //clicks
          | 
          | spacings('time) :=
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
          | spacings.click where spacings.below > meanBelow | spacings.above > meanAbove""".stripMargin

        todo //eval(input) must not(beEmpty)   
      }

      "forall expression" >> {
        val input = """
          | clicks := //clicks
          | 
          | spacings := (forall 'time
          |   click := clicks where clicks.time = 'time
          |   belowTime := max(clicks.time where clicks.time < 'time)
          |   aboveTime := min(clicks.time where clicks.time > 'time)
          |   
          |   {
          |     click: click,
          |     below: click.time - belowTime,
          |     above: aboveTime - click.time
          |   })
          |   
          | meanAbove := mean(spacings.above)
          | meanBelow := mean(spacings.below)
          | 
          | spacings.click where spacings.below > meanBelow | spacings.above > meanAbove""".stripMargin

        
        todo //eval(input) must not(beEmpty) 
      }
    }
  
    "evaluate the 'hello, quirrel' examples" >> {
      "json" >> {
        "object" >> {
          val result = eval("""{ name: "John", age: 29, gender: "male" }""")
          result must haveSize(1)
          result must contain(SObject(Map("name" -> SString("John"), "age" -> SDecimal(29), "gender" -> SString("male"))))
        }
        
        "object with null" >> {
          val result = eval("""{ name: "John", age: 29, gender: null }""")
          result must haveSize(1)
          result must contain(SObject(Map("name" -> SString("John"), "age" -> SDecimal(29), "gender" -> SNull)))
        }
        
        "boolean" >> {
          val result = eval("true")
          result must haveSize(1)
          result must contain(SBoolean(true))
        }
        
        "string" >> {
          val result = eval("\"hello, world\"")
          result must haveSize(1)
          result must contain(SString("hello, world"))
        }        

        "null" >> {
          val result = eval("null")
          result must haveSize(1)
          result must contain(SNull)
        }
      }
      
      "numbers" >> {
        "addition" >> {
          val result = eval("5 + 2")
          result must haveSize(1)
          result must contain(SDecimal(7))
        }
        
        "multiplication" >> {
          val result = eval("8 * 2")
          result must haveSize(1)
          result must contain(SDecimal(16))
        }
      }
      
      "booleans" >> {
        "greater-than" >> {
          val result = eval("5 > 2")
          result must haveSize(1)
          result must contain(SBoolean(true))
        }
        
        "not-equal" >> {
          val result = eval("\"foo\" != \"foo\"")
          result must haveSize(1)
          result must contain(SBoolean(false))
        }
      }
      
      "variables" >> {
        "1" >> {
          val input = """
            | total := 2 + 1
            | total * 3""".stripMargin
            
          val result = eval(input)
          result must haveSize(1)
          result must contain(SDecimal(9))
        }
        
        "2" >> {
          val input = """
            | num := 4
            | square := num * num
            | square - 1""".stripMargin
            
          val result = eval(input)
          result must haveSize(1)
          result must contain(SDecimal(15))
        }
      }

      "outliers" >> {
        val input = """
           | campaigns := //campaigns
           | bound := stdDev(campaigns.cpm)
           | avg := mean(campaigns.cpm)
           | outliers := campaigns where campaigns.cpm > (avg + bound)
           | outliers.platform""".stripMargin

          val result = eval(input)
          result must haveSize(5)
      }
      
      "should merge objects without timing out" >> {
        val input = """
           //richie1/test 
        """.stripMargin

        eval(input) must not(throwA[Throwable])
      }.pendingUntilFixed

      "handle query on empty array" >> {
        val input = """
          //test/empty_array
        """.stripMargin

        eval(input) mustEqual Set(SArray(Vector()), SObject(Map("foo" -> SArray(Vector()))))
      }.pendingUntilFixed
      
      "handle query on empty object" >> {
        val input = """
          //test/empty_object
        """.stripMargin

        eval(input) mustEqual Set(SObject(Map()), SObject(Map("foo" -> SObject(Map()))))
      }.pendingUntilFixed

      "handle query on null" >> {
        val input = """
          //test/null
        """.stripMargin

        eval(input) mustEqual Set(SNull, SObject(Map("foo" -> SNull)))
      }

      "handle filter on null" >> {
        val input = """
          //fastspring_nulls where //fastspring_nulls.endDate = null
        """.stripMargin

        val result = eval(input) 
        result must haveSize(1)
      }

      "handle load of error-prone fastspring data" >> {
        (eval("//fastspring_nulls") must haveSize(2)) and
        (eval("//fastspring_mixed_type") must haveSize(2))
      }.pendingUntilFixed

      // times out...
      /* "handle chained characteristic functions" in {
        val input = """
          | cust := //fs1/customers
          | tran := //fs1/transactions
          | relations('customer) :=
          |   cust' := cust where cust.customer = 'customer
          |   tran' := tran where tran.customer = 'customer
          |   tran' ~ cust'
          |     { country : cust'.country,  time : tran'.time, quantity : tran'.quantity }
          | grouping('country) :=
          |   { country: 'country, count: sum((relations where relations.country = 'country).quantity) }
          | grouping""".stripMargin

        val result = eval(input)
        result must haveSize(4)
      } */
    }
  }
}

// vim: set ts=4 sw=4 et:
