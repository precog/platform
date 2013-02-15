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
package actor

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.json._

import blueeyes.json._

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, MatchResult, Expectable}

import scala.collection.immutable.ListMap
import scala.math.BigDecimal

class RoutingTableSpec extends Specification {
  import ProjectionInsert.Row
  
  "SingleColumnProjectionRoutingTable" should {

    def toProjDesc(colDescs: List[ColumnDescriptor]) = ProjectionDescriptor(1, colDescs)

    "project an event with one property to a single projection action" in {
      val rt = new SingleColumnProjectionRoutingTable
      
      val jval = JObject(
        JField("selector", JString("Test")) :: Nil 
      )

      val eventId = EventId(0, 0)
      val msg = IngestMessage("apiKey", Path("/a/b"), "someOwner", Vector(IngestRecord(eventId, jval)), None)
      
      val colDesc = ColumnDescriptor(Path("/a/b/"), CPath(".selector"), CString, Authorities(Set("someOwner")))

      val actions = rt.routeIngest(msg)

      val expected = Seq(
        ProjectionInsert(toProjDesc(colDesc :: Nil), Seq(Row(eventId, List[CValue](CString("Test")), Nil)))
      )

      actions must containAllOf(expected).only
    }

    "project an event with n properties to n projection actions" in {
      val rt = new SingleColumnProjectionRoutingTable

      val jval = JObject(
        JField("selector", JString("Test")),
        JField("foo", JObject( JField("bar", JNum(123)),
                               JField("bat", JNum(456.78)),
                               JField("baz", JNum(BigDecimal("91011.12E+1314")))))
      )

      val eventId = EventId(0, 0)
      val msg = IngestMessage("apiKey", Path("/a/b"), "someOwner", Vector(IngestRecord(eventId, jval)), None)

      val colDesc1 = ColumnDescriptor(Path("/a/b/"), CPath(".selector"), CString, Authorities(Set("someOwner")))
      val colDesc2 = ColumnDescriptor(Path("/a/b/"), CPath(".foo.bar"), CLong, Authorities(Set("someOwner")))
      val colDesc3 = ColumnDescriptor(Path("/a/b/"), CPath(".foo.bat"), CDouble, Authorities(Set("someOwner")))
      val colDesc4 = ColumnDescriptor(Path("/a/b/"), CPath(".foo.baz"), CNum, Authorities(Set("someOwner")))

      val actions = rt.routeIngest(msg)

      val expected = Seq(
        ProjectionInsert(toProjDesc(colDesc1 :: Nil), Seq(Row(eventId, VectorCase[CValue](CString("Test")), Nil))),
        ProjectionInsert(toProjDesc(colDesc2 :: Nil), Seq(Row(eventId, VectorCase[CValue](CLong(123)), Nil))),
        ProjectionInsert(toProjDesc(colDesc3 :: Nil), Seq(Row(eventId, VectorCase[CValue](CDouble(456.78)), Nil))),
        ProjectionInsert(toProjDesc(colDesc4 :: Nil), Seq(Row(eventId, VectorCase[CValue](CNum(BigDecimal("91011.12E+1314"))), Nil)))
      )

      actions must containAllOf(expected).only
    }
  }
}
