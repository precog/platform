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

import com.precog.bytecode._
import com.precog.common.json._
import scala.util.Random

import blueeyes.json._

import scalaz.StreamT
import scalaz.syntax.monad._
import scalaz.syntax.copointed._
import scalaz.std.string._

import org.specs2.ScalaCheck
import org.specs2.mutable._

trait PartitionMergeSpec[M[+_]] extends ColumnarTableModuleTestSupport[M] with Specification with ScalaCheck {
  import SampleData._
  import trans._

  def testPartitionMerge = {
    val JArray(elements) = JParser.parse("""[
      { "key": [0], "value": { "a": "0a" } },
      { "key": [1], "value": { "a": "1a" } },
      { "key": [1], "value": { "a": "1b" } },
      { "key": [1], "value": { "a": "1c" } },
      { "key": [2], "value": { "a": "2a" } },
      { "key": [3], "value": { "a": "3a" } },
      { "key": [3], "value": { "a": "3b" } },
      { "key": [4], "value": { "a": "4a" } }
    ]""")

    val tbl = fromJson(elements.toStream)

    val JArray(expected) = JParser.parse("""[
      "0a",
      "1a;1b;1c",
      "2a",
      "3a;3b",
      "4a" 
    ]""")

    val result: M[Table] = tbl.partitionMerge(DerefObjectStatic(Leaf(Source), CPathField("key"))) { table =>
      val reducer = new Reducer[String] {
        def reduce(columns: JType => Set[Column], range: Range): String = {
          columns(JTextT).head match {
            case col: StrColumn => range.map(col).mkString(";")
          }
        }
      }

      val derefed = table.transform(DerefObjectStatic(DerefObjectStatic(Leaf(Source), CPathField("value")), CPathField("a")))
      
      derefed.reduce(reducer).map(s => Table.constString(Set(CString(s))))
    }

    result.flatMap(_.toJson).copoint must_== expected.toStream
  }

}
  

// vim: set ts=4 sw=4 et:
