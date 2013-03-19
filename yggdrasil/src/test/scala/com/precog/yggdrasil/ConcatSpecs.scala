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
package table

import org.specs2.mutable._

import scalaz.syntax.comonad._

import blueeyes.json._

trait ConcatSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification {
  def testConcat = {
    val json1 = """{ "a": 1, "b": "x", "c": null }"""
    val json2 = """[4, "foo", null, true]"""

    val data1: Stream[JValue] = Stream.fill(25)(JParser.parse(json1))
    val data2: Stream[JValue] = Stream.fill(35)(JParser.parse(json2))
    
    val table1 = fromSample(SampleData(data1), Some(10))
    val table2 = fromSample(SampleData(data2), Some(10))

    val results = toJson(table1.concat(table2))
    val expected = data1 ++ data2

    results.copoint must_== expected
  }
}
