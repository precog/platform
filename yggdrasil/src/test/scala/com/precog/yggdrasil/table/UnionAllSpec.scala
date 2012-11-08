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

import org.specs2.mutable.Specification

import com.precog.common.json._
import blueeyes.json._

import scalaz._
import scalaz.syntax.copointed._

trait UnionAllSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification {
  import Table._

  override type GroupId = Int

  object unionAllData {
    val JArray(leftData) = JParser.parse("""[
      {
        "groupKeys":  { "%1$s": "foo", "%2$s": false },
        "identities": { "1": [1,2] },
        "values":     { "1": { "a": "foo", "b": false } }
      },
      {
        "groupKeys":  { "%1$s": "foo", "%2$s": true },
        "identities": { "1": [1,3] },
        "values":     { "1": { "a": "foo", "b": true } }
      }
    ]""".format(GroupKeyTrans.keyName(0), GroupKeyTrans.keyName(1)))
  
    val JArray(rightData) = JParser.parse("""[
      {
        "groupKeys":  { "%1$s": "bar", "%2$s": true },
        "identities": { "1": [5,1] },
        "values":     { "1": { "a": "bar", "b": true } }
      },
      {
        "groupKeys":  { "%1$s": "baz", "%2$s": true },
        "identities": { "1": [4,5] },
        "values":     { "1": { "a": "baz", "b": true } }
      }
    ]""".format(GroupKeyTrans.keyName(0), GroupKeyTrans.keyName(1)))
  
    val JArray(rightDataReversed) = JParser.parse("""[
      {
        "groupKeys":  { "%2$s": "bar", "%1$s": true },
        "identities": { "1": [5,1] },
        "values":     { "1": { "a": "bar", "b": true } }
      },
      {
        "groupKeys":  { "%2$s": "baz", "%1$s": true },
        "identities": { "1": [4,5] },
        "values":     { "1": { "a": "baz", "b": true } }
      }
    ]""".format(GroupKeyTrans.keyName(0), GroupKeyTrans.keyName(1)))
  }

  def simpleUnionAllTest = {
    import unionAllData._
    val vars = Seq(CPathField("a"), CPathField("b"))
  
    val leftBorg = BorgResult(fromJson(leftData.toStream), vars, Set(1), UnknownSize)
    val rightBorg = BorgResult(fromJson(rightData.toStream), vars, Set(1), UnknownSize)

    val expected = leftData.toStream ++ rightData.toStream

    val result = unionAll(Set(leftBorg, rightBorg))
    val jsonResult = result.table.toJson.copoint

    jsonResult       must_== expected
    result.groupKeys must_== vars
    result.groups    must_== Set(1)
  }

  def reversedUnionAllTest = {
    import unionAllData._
    val varsLeft = Seq(CPathField("a"), CPathField("b"))
    val varsRight = Seq(CPathField("b"), CPathField("a"))
  
    val leftBorg = BorgResult(fromJson(leftData.toStream), varsLeft, Set(1), UnknownSize)
    val rightBorg = BorgResult(fromJson(rightDataReversed.toStream), varsRight, Set(1), UnknownSize)

    val expected = leftData.toStream ++ rightData.toStream

    val result = unionAll(Set(leftBorg, rightBorg))
    val jsonResult = result.table.toJson.copoint

    jsonResult       must_== expected
    result.groupKeys must_== varsLeft
    result.groups    must_== Set(1)
  }
}
