package com.precog
package muspelheim

import com.precog.yggdrasil._

trait HelloQuirrelSpecs extends EvalStackSpecs {
  import stack._
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

        "object with undefined" >> {
          val result = eval("""{ name: "John", age: 29, gender: undefined }""")
          result must haveSize(0)
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

        "undefined" >> {
          val result = eval("undefined")
          result must haveSize(0)
        }
      }

      "numbers" >> {
        "addition" >> {
          val result = eval("5 + 2")
          result must haveSize(1)
          result must contain(SDecimal(7))
        }

        "subtraction" >> {
          val result = eval("5 - 2")
          result must haveSize(1)
          result must contain(SDecimal(3))
        }

        "multiplication" >> {
          val result = eval("8 * 2")
          result must haveSize(1)
          result must contain(SDecimal(16))
        }

        "division" >> {
          val result = eval("12 / 3")
          result must haveSize(1)
          result must contain(SDecimal(4))
        }

        "mod" >> {
          val result = eval("5 % 2")
          result must haveSize(1)
          result must contain(SDecimal(1))
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

      "undefineds" >> {
        "addition" >> {
          val result = eval("5 + undefined")
          result must haveSize(0)
        }

        "greater-than" >> {
          val result = eval("5 > undefined")
          result must haveSize(0)
        }

        // note that `5 intersect undefined` is provably empty
        // and thus kicked out by the compiler
        "union" >> {
          val result = eval("5 union undefined")
          result must haveSize(1)
          result must contain(SDecimal(5))
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

        eval(input) must haveSize(100)
      }

      "handle filter on null" >> {
        val input = """
          //fastspring_nulls where (//fastspring_nulls).endDate = null
        """.stripMargin

        val result = eval(input)
        result must haveSize(1)
      }

      "handle load of error-prone fastspring data" >> {
        eval("//fastspring_nulls") must haveSize(2)
        eval("//fastspring_mixed_type") must haveSize(2)
      }

      "count the obnoxiously large dataset" >> {
        "<root>" >> {
          eval("count((//obnoxious).v)") mustEqual Set(SDecimal(100000))
        }
      }


      // FIXME: This is no longer proper syntax.
//      "handle chained characteristic functions" in {
//        val input = """
//          | cust := //fs1/customers
//          | tran := //fs1/transactions
//          | relations('customer) :=
//          |   cust' := cust where cust.customer = 'customer
//          |   tran' := tran where tran.customer = 'customer
//          |   tran' ~ cust'
//          |     { country : cust'.country,  time : tran'.time, quantity : tran'.quantity }
//          | grouping('country) :=
//          |   { country: 'country, count: sum((relations where relations.country = 'country).quantity) }
//          | grouping""".stripMargin
//
//        val result = eval(input)
//        result must haveSize(4)
//      }
    }
}
