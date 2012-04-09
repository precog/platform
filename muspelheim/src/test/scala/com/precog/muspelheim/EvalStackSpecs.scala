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
        | clicks := load(//clicks)
        | count(clicks where clicks.time > 0)""".stripMargin
        
      eval(input) mustEqual Set(SDecimal(100))
    }

    "count the campaigns dataset" >> {
      "<root>" >> {
        eval("count(load(//campaigns))") mustEqual Set(SDecimal(100))
      }
      
      "gender" >> {
        eval("count(load(//campaigns).gender)") mustEqual Set(SDecimal(100))
      }
      
      "platform" >> {
        eval("count(load(//campaigns).platform)") mustEqual Set(SDecimal(100))
      }
      
      "campaign" >> {
        eval("count(load(//campaigns).campaign)") mustEqual Set(SDecimal(100))
      }
      
      "cpm" >> {
        eval("count(load(//campaigns).cpm)") mustEqual Set(SDecimal(100))
      }

      "ageRange" >> {
        eval("count(load(//campaigns).ageRange)") mustEqual Set(SDecimal(100))
      }
    }

    "use the where operator on a unioned set" >> {
      "campaigns.gender" >> {
        val input = """
          | a := load(//campaigns) union load(//clicks)
          |   a where a.gender = "female" """.stripMargin
          
        val results = evalE(input)
        
        results must haveSize(46)
        
        forall(results) {
          case (VectorCase(_), SObject(obj)) => {
            obj must haveSize(5)
            obj must contain("gender" -> SString("female"))
          }
        }
      }

      "clicks.platform" >> {
        val input = """
          | a := load(//campaigns) union load(//clicks)
          |   a where a.platform = "android" """.stripMargin
          
        val results = evalE(input)
        
        results must haveSize(72)
        
        forall(results) {
          case (VectorCase(_), SObject(obj)) => {
            obj must haveSize(5)
            obj must contain("platform" -> SString("android"))
          }
        }
      }
    }

    "use the where operator on an intersected set" >> {
      "campaigns.gender" >> {
        val input = """
          | a := load(//campaigns).campaign union load(//campaigns).cpm
          |   a intersect load(//campaigns).campaign """.stripMargin
          
        val results = evalE(input)
        
        results must haveSize(100)
        
        forall(results) {
          case (VectorCase(_), SString(campaign)) =>
            Set("c16","c9","c21","c15","c26","c5","c18","c7","c4","c17","c11","c13","c12","c28","c23","c14","c10","c19","c6","c24","c22","c20") must contain(campaign)
        }
      }

      "clicks.platform" >> {
        val input = """
          | a := load(//campaigns).campaign union load(//campaigns).cpm
          |   a intersect load(//campaigns).cpm """.stripMargin
          
        val results = evalE(input)
        
        results must haveSize(100)
        
        forall(results) {
          case (VectorCase(_), SDecimal(num)) => {
            Set(100,39,91,77,96,99,48,67,10,17,90,58,20,38,1,43,49,23,72,42,94,16,9,21,52,5,40,62,4,33,28,54,70,82,76,22,6,12,65,31,80,45,51,89,69) must contain(num)
          }
        }
      }
    }

    "use the where operator on a key with string values" in {
      val input = """load(//campaigns) where load(//campaigns).platform = "android" """
      val results = evalE(input)
      
      results must haveSize(72)

      forall(results) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(5)
          obj must contain("platform" -> SString("android"))
        }
      }
    }

    "use the where operator on a key with numeric values" in {
      val input = "load(//campaigns) where load(//campaigns).cpm = 1 "
      val results = evalE(input)
      
      results must haveSize(34)

      forall(results) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(5)
          obj must contain("cpm" -> SDecimal(1))
        }
      }
    }

    "use the where operator on a key with array values" in {
      val input = "load(//campaigns) where load(//campaigns).ageRange = [37, 48]"
      val results = evalE(input)
      
      results must haveSize(39)

      forall(results) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(5)
          obj must contain("ageRange" -> SArray(Vector(SDecimal(37), SDecimal(48))))
        }
      }
    }

    "evaluate the with operator across the campaigns dataset" in {
      val input = "count(load(//campaigns) with { t: 42 })"
      eval(input) mustEqual Set(SDecimal(100))
    }

    "perform distinct" >> {
      "on a homogenous set of numbers" >> {
        val input = """
          | a := load(//campaigns)
          |   distinct(a.gender)""".stripMargin

        eval(input) mustEqual Set(SString("female"), SString("male"))   
      }

      "on set of strings formed by a union" >> {
        val input = """
          | gender := load(//campaigns).gender
          | pageId := load(//clicks).pageId
          | distinct(gender union pageId)""".stripMargin

        eval(input) mustEqual Set(SString("female"), SString("male"), SString("page-0"), SString("page-1"), SString("page-2"), SString("page-3"), SString("page-4"))   
      }
    }

    "map object creation over the campaigns dataset" in {
      val input = "{ aa: load(//campaigns).campaign }"
      val results = evalE(input)
      
      results must haveSize(100)
      
      forall(results) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(1)
          obj must haveKey("aa")
        }
      }
    }
    
    "perform a naive cartesian product on the campaigns dataset" in {
      val input = """
        | a := load(//campaigns)
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
      }
    }    

    "add sets of different types" >> {
      "a set of numbers and a set of strings" >> {
        val input = "load(//campaigns).cpm + load(//campaigns).gender"

        eval(input) mustEqual Set()
      }

      "a set of numbers and a set of arrays" >> {
        val input = "load(//campaigns).cpm + load(//campaigns).ageRange"

        eval(input) mustEqual Set()
      }

      "a set of arrays and a set of strings" >> {
        val input = "load(//campaigns).gender + load(//campaigns).ageRange"

        eval(input) mustEqual Set()
      }
    }

    "return only all possible value results from a characteristic function" in {
      val input = """
        | campaigns := load(//campaigns)
        | f('a) :=
        |   campaigns.gender where campaigns.platform = 'a
        |
        | f""".stripMargin
        
      val results = evalE(input)
      
      results must haveSize(100)
      
      forall(results) {
        case (VectorCase(_), SString(gender)) =>
          gender must beOneOf("male", "female")
      }
    }
    
    "determine a histogram of genders on campaigns" in {
      val input = """
        | campaigns := load(//campaigns)
        | hist('gender) :=
        |   { gender: 'gender, num: count(campaigns.gender where campaigns.gender = 'gender) }
        | hist""".stripMargin
        
      eval(input) mustEqual Set(
        SObject(Map("gender" -> SString("female"), "num" -> SDecimal(46))),
        SObject(Map("gender" -> SString("male"), "num" -> SDecimal(54))))
    }

    "load a nonexistent dataset with a dot in the name" in {
      val input = """
        | load(//foo.bar)""".stripMargin
     
      eval(input) mustEqual Set()
    }

    "evaluate functions from each library" >> {
      "Stringlib" >> {
        val input = """
          | gender := distinct(load(//campaigns).gender)
          | std::string::concat("alpha ", gender)""".stripMargin

        eval(input) mustEqual Set(SString("alpha female"), SString("alpha male"))
      }

      "Mathlib" >> {
        val input = """
          | cpm := distinct(load(//campaigns).cpm)
          | selectCpm := cpm where cpm < 10
          | std::math::pow(selectCpm, 2)""".stripMargin

        eval(input) mustEqual Set(SDecimal(25), SDecimal(1), SDecimal(36), SDecimal(81), SDecimal(16))
      }

      "Timelib" >> {
        val input = """
          | time := load(//clicks).timeString
          | std::time::yearsBetween(time, "2012-02-09T19:31:13.616+10:00")""".stripMargin

        val results = evalE(input) 
        val results2 = results map { case (VectorCase(_), SDecimal(d)) => d.toInt } 

        results2 must contain(0).only
      }
    }
 
    "set critical conditions given an empty set" in {
        val input = """
          | function('a) :=
          |   load(//campaigns) where load(//campaigns).foo = 'a
          | function""".stripMargin

        eval(input) mustEqual Set()
    }

    "use NotEq correctly" in {
      val input = """load(//campaigns) where load(//campaigns).gender != "female" """.stripMargin

      val results = evalE(input)

      forall(results) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(5)
          obj must contain("gender" -> SString("male"))
        }
      }
    }

    "evaluate an unquantified characteristic function" in {
      val input = """
        | campaigns := load(//campaigns)
        | nums := distinct(campaigns.cpm where campaigns.cpm < 10)
        | sums('n) :=
        |   m := max(nums where nums < 'n)
        |   (nums where nums = 'n) + m 
        | sums""".stripMargin

      eval(input) mustEqual Set(SDecimal(15), SDecimal(11), SDecimal(9), SDecimal(5))
    }

    "evaluate a quantified characteristic function of two parameters" in {
      val input = """
        | fun('a, 'b) := 
        |   load(//campaigns) where load(//campaigns).ageRange = 'a & load(//campaigns).gender = 'b
        | fun([25,36], "female")""".stripMargin

      val results = evalE(input) 
      
      forall(results) {
        case (VectorCase(_), SObject(obj)) => {
          obj must haveSize(5)
          obj must contain("ageRange" -> SArray(Vector(SDecimal(25), SDecimal(36))))
          obj must contain("gender" -> SString("female"))
        }
      }
    }

    "evaluate an unquantified characteristic function of two parameters" in {  //note: this is NOT the the most efficient way to implement this query, but it still should work
      val input = """
        | campaigns := load(//campaigns)
        | gender := campaigns.gender
        | platform := campaigns.platform
        | equality('a, 'b) :=
        |   g := gender where gender = 'a
        |   p := platform where platform = 'b
        |   campaigns where g = p
        | equality""".stripMargin

      eval(input) mustEqual Set()
    }

    "determine a histogram of genders on category" in {
      val input = """
        | campaigns := load(//campaigns)
        | organizations := load(//organizations)
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

      eval(input) mustEqual Set()   // TODO
    }.pendingUntilFixed
     
    "determine most isolated clicks in time" in {
      val input = """
        | clicks := load(//clicks)
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

      eval(input) must not(beEmpty)   // TODO
    }
  
    "evaluate the 'hello, quirrel' examples" >> {
      "json" >> {
        "object" >> {
          val result = eval("""{ name: "John", age: 29, gender: "male" }""")
          result must haveSize(1)
          result must contain(SObject(Map("name" -> SString("John"), "age" -> SDecimal(29), "gender" -> SString("male"))))
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
           | campaigns := load(//campaigns)
           | bound := stdDev(campaigns.cpm)
           | avg := mean(campaigns.cpm)
           | outliers := campaigns where campaigns.cpm > (avg + bound)
           | outliers.platform""".stripMargin

          val result = eval(input)
          result must haveSize(5)
      }
      
      "should merge objects without timing out" >> {
        val input = """
           load(//richie1/test) 
        """.stripMargin

        eval(input) must not(throwA[Throwable])
      }

      "handle query on empty array" >> {
        val input = """
          load(//test/empty_array)
        """.stripMargin
        
        eval(input) must not(throwA[Throwable])
      }.pendingUntilFixed

      // times out...
      /* "handle chained characteristic functions" in {
        val input = """
          | cust := load(//fs1/customers)
          | tran := load(//fs1/transactions)
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
