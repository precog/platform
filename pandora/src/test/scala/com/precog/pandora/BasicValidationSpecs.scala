package com.precog
package pandora

import com.precog.yggdrasil._

class BasicValidationSpecs extends PlatformSpec {
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
  }
}
