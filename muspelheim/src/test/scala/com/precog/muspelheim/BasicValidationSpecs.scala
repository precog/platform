package com.precog
package muspelheim

import com.precog.bytecode._
import com.precog.yggdrasil._
import com.precog.daze._

trait BasicValidationSpecs extends EvalStackSpecs with Instructions {
  "Fundamental stack support" should {
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
        eval("count((//campaigns).gender)") mustEqual Set(SDecimal(100))
      }

      "platform" >> {
        eval("count((//campaigns).platform)") mustEqual Set(SDecimal(100))
      }

      "campaign" >> {
        eval("count((//campaigns).campaign)") mustEqual Set(SDecimal(100))
      }

      "cpm" >> {
        eval("count((//campaigns).cpm)") mustEqual Set(SDecimal(100))
      }

      "ageRange" >> {
        eval("count((//campaigns).ageRange)") mustEqual Set(SDecimal(100))
      }
    }

    "reduce the obnoxiously large dataset" >> {
      "<root>" >> {
        eval("mean((//obnoxious).v)") mustEqual Set(SDecimal(50000.5))
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
      }
    }

    "accept a dereferenced object" >> {
      "non-empty object" >> {
        eval("{a: 42}[1]") mustEqual Set()
      }

      "empty object" >> {
        eval("{}[0]") mustEqual Set()
      }
    }

    "accept a where'd empty array and empty object" >> {
      "empty object (left)" >> {
        eval("{} where true") mustEqual Set(SObject(Map()))
      }

      "empty object (right)" >> {
        eval("true where {}") mustEqual Set()
      }

      "empty array (left)" >> {
        eval("[] where true") mustEqual Set(SArray(Vector()))
      }

      "empty array (right)" >> {
        eval("true where []") mustEqual Set()
      }
    }

    "accept a with'd empty array and empty object" >> {
      "empty object (left)" >> {
        eval("{} with true") mustEqual Set()
      }

      "empty object (right)" >> {
        eval("true with {}") mustEqual Set()
      }

      "empty array (left)" >> {
        eval("[] with true") mustEqual Set()
      }

      "empty array (right)" >> {
        eval("true with []") mustEqual Set()
      }
    }

    "produce a result with a passed assertion" in {
      eval("assert true 42") mustEqual Set(SDecimal(42))
    }

    "throw an exception with a failed assertion" in {
      import instructions.Line

      val input = """
        | a := 42
        | assert false a
        | """.stripMargin

      eval(input) must throwA[FatalQueryException[Line]].like {
        case e => e must beLike {
          case FatalQueryException(Line(3, 2, " assert false a"), "Assertion failed") => ok
        }
      }
    }

    "correctly evaluate forall" in {
      eval("forall(true union false)") mustEqual Set(SBoolean(false))
    }

    "correctly evaluate exists" in {
      eval("exists(true union false)") mustEqual Set(SBoolean(true))
    }

    "flatten an array into a set" in {
      eval("flatten([1, 2, 3])") mustEqual Set(SDecimal(1), SDecimal(2), SDecimal(3))
    }
  }
}
