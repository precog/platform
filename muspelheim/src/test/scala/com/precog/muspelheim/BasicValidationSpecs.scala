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

import com.precog.bytecode._
import com.precog.yggdrasil._
import com.precog.daze._

trait BasicValidationSpecs extends EvalStackSpecs {
  type TestStack <: EvalStackLike with Instructions
  import stack._

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

      eval(input) must throwA[FatalQueryException].like {
        case e => e must beLike {
          case FatalQueryException(_) => ok
          // TODO: Check error channel for message.
          // case FatalQueryException(Line(3, 2, " assert false a"), "Assertion failed") => ok
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
