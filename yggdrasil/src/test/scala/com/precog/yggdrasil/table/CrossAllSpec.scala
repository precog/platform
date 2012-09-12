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

import blueeyes.json.JPathField
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser

import scalaz._
import scalaz.syntax.copointed._

trait CrossAllSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification {
  import Table._

  override type GroupId = Int

  object crossAllData {
    // These are used in all tests
    val JArray(leftData) = JsonParser.parse("""[
      {
        "groupKeys":  { "%1$s": "foo" },
        "identities": { "1": [1,2] },
        "values":     { "1": { "a": "foo" } }
      }
    ]""".format(GroupKeyTrans.keyName(0)))
  
    val JArray(rightData) = JsonParser.parse("""[
      {
        "groupKeys":  { "%1$s": true },
        "identities": { "2": [5,1] },
        "values":     { "2": { "b": true } }
      }
    ]""".format(GroupKeyTrans.keyName(0)))

    val JArray(crossedData) = JsonParser.parse("""[
      {
        "groupKeys":  { "%1$s": "foo", "%2$s" : true },
        "identities": { "1": [1,2], "2": [5,1] },
        "values":     { "1": { "a": "foo" }, "2": { "b": true } }
      }
    ]""".format(GroupKeyTrans.keyName(0), GroupKeyTrans.keyName(1)))
    
  }

  def simpleCrossAllTest = {
    import crossAllData._
    val varsLeft = Seq(JPathField("a"))
    val varsRight = Seq(JPathField("b"))
  
    val leftBorg = BorgResult(fromJson(leftData.toStream), varsLeft, Set(1))
    val rightBorg = BorgResult(fromJson(rightData.toStream), varsRight, Set(1))

    val result = crossAll(Set(leftBorg, rightBorg))
    val jsonResult = result.table.toJson.copoint

    jsonResult       must_== crossedData
    result.groupKeys must_== varsLeft ++ varsRight
    result.groups    must_== Set(1)
  }
}
