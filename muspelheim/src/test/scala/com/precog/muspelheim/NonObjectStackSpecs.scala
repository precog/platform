package com.precog
package muspelheim

import com.precog.yggdrasil._

// Currently this is only supported by the full Precog backend
trait NonObjectStackSpecs extends EvalStackSpecs {
  import stack._
  "Non-object values" should {
    "handle query on empty array" >> {
      val input = """
          //test/empty_array
        """.stripMargin

      eval(input) mustEqual Set(SArray(Vector()), SObject(Map("foo" -> SArray(Vector()))))
    }
    
    "handle query on empty object" >> {
      val input = """
          //test/empty_object
        """.stripMargin

      eval(input) mustEqual Set(SObject(Map()), SObject(Map("foo" -> SObject(Map()))))
    }

    "handle query on null" >> {
      val input = """
          //test/null
        """.stripMargin

      eval(input) mustEqual Set(SNull, SObject(Map("foo" -> SNull)))
    }
  }
}

