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
package com.precog.yggdrasil

import com.precog.bytecode._

import com.precog.common._

import org.specs2.mutable.Specification

class SchemaSpec extends Specification {
  "cpath" should {
    "return the correct sequence of CPath" in {
      val jtype = JObjectFixedT(Map(
        "foo" -> JNumberT, 
        "bar" -> JArrayFixedT(Map(
          0 -> JBooleanT, 
          1 -> JObjectFixedT(Map(
            "baz" -> JArrayHomogeneousT(JNullT))),
          2 -> JTextT))))

      val result = Schema.cpath(jtype)
      val expected = Seq(
        CPath(CPathField("foo")),
        CPath(CPathField("bar"), CPathIndex(0)),
        CPath(CPathField("bar"), CPathIndex(1), CPathField("baz"), CPathArray),
        CPath(CPathField("bar"), CPathIndex(2))) sorted
        
      result mustEqual expected
    }
  }
}
