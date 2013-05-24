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

