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

import akka.dispatch.{Await, Future, ExecutionContext}
import akka.util.Duration

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._

import java.util.concurrent.Executors

import org.specs2._
import org.specs2.mutable.Specification
import org.specs2.ScalaCheck

import scalaz.std.anyVal._

object GrouperSpec extends Specification with table.StubColumnarTableModule {
  import trans._
  
  implicit val asyncContext = ExecutionContext fromExecutor Executors.newCachedThreadPool()
  
  "grouping" should {
    "compute a histogram by value" in {
      val set = Stream(
        77,
        42,
        22,
        77,
        12,
        22,
        15,
        15,
        -71,
        22,
        42,
        77,
        22,
        0,
        15,
        53,
        -41,
        22,
        -41,
        0,
        22,
        77,
        69,
        42,
        0,
        53,
        22,
        22)
      
      val data = set map { JInt(_) }
        
      val spec = fromJson(data).group(TransSpec1.Id, 2,
        GroupKeySpecSource(JPathField("1"), TransSpec1.Id))
        
      val result = grouper.merge(spec) { (key: Table, map: Int => Table) =>
        val keyIter = key.toJson
        
        keyIter must haveSize(1)
        keyIter.head must beLike {
          case JInt(i) => set must contain(i)
        }
        
        val setIter = map(2).toJson
        
        setIter must not(beEmpty)
        forall(setIter) { i =>
          i mustEqual keyIter.head
        }
        
        Future(fromJson(JInt(setIter.size) #:: Stream.empty))
      }
      
      val resultIter = Await.result(result, Duration(10, "seconds")).toJson
      
      resultIter must haveSize(set.distinct.size)
      
      val expectedSet = (set.toSeq groupBy identity values) map { _.length } map { JInt(_) }
      
      forall(resultIter) { i => expectedSet must contain(i) }
    }.pendingUntilFixed
  }
}
