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

import com.precog.common.VectorCase
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser.parse
import blueeyes.json.JPathField

import scalaz.syntax.bind._
import scalaz.syntax.copointed._

trait IntersectSpec[M[+_]] extends TableModuleSpec[M] {
  import SampleData._
  import trans._
  import trans.constants._

  def testIntersect(l: SampleData, r: SampleData) {
    val ltable = fromSample(l)
    val rtable = fromSample(r)

    val expected: Stream[JValue] = for {
      lv <- l.data
      rv <- r.data
      if (lv \ "key") == (rv \ "key")
    } yield lv

    val result = ops.intersect(DerefObjectStatic(Leaf(Source), JPathField("key")), ltable, rtable)

    val jsonResult: M[Stream[JValue]] = result.flatMap { table => toJson(table) }

    jsonResult.copoint must_== expected
  }

  def testSimpleIntersect = {
    val s1 = SampleData(Stream(toRecord(VectorCase(1), parse("""{"a":[]}""")), toRecord(VectorCase(2), parse("""{"b":[]}"""))))
    val s2 = SampleData(Stream(toRecord(VectorCase(2), parse("""{"b":[]}"""))))

    testIntersect(s1, s2)
  }
}
    
