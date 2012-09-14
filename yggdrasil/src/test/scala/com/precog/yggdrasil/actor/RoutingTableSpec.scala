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
import com.precog.common.json._

import blueeyes.json.JsonAST._
import blueeyes.json.JPath

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, MatchResult, Expectable}

import scala.collection.immutable.ListMap
import scala.math.BigDecimal

class RoutingTableSpec extends Specification {
  
  "SingleColumnProjectionRoutingTable" should {

    def toProjDesc(colDescs: List[ColumnDescriptor]) = ProjectionDescriptor(1, colDescs)

    "project an event with one property to a single projection action" in {
      val rt = new SingleColumnProjectionRoutingTable
      
      val jval = JObject(
        JField("selector", JString("Test")) :: Nil 
      )

      val metadata = Map[JPath, Set[UserMetadata]]() +
                     (JPath(".selector") -> Set.empty[UserMetadata])
      
      val msg = EventMessage(EventId(0,0), Event(Path("/a/b"), "token", jval, metadata))
      
      val colDesc = ColumnDescriptor(Path("/a/b/"), CPath(".selector"), CString, Authorities(Set("token")))

      val actions = rt.routeEvent(msg)

      val expected = Seq(ProjectionData(toProjDesc(colDesc :: Nil), List[CValue](CString("Test")), List(Set.empty)))

      actions must containAllOf(expected).only
    }

    "project an event with n properties to n projection actions" in {
      val rt = new SingleColumnProjectionRoutingTable

      val jval = JObject(
        JField("selector", JString("Test")) ::
        JField("foo", JObject( JField("bar", JNum(123)) :: Nil )) ::
        JField("foo", JObject( JField("bat", JNum(456.78)) :: Nil )) ::
        JField("foo", JObject( JField("baz", JNum(BigDecimal("91011.12E+1314"))) :: Nil )) :: Nil
      )

      val metadata = Map[JPath, Set[UserMetadata]]() +
                     (JPath(".selector") -> Set.empty[UserMetadata]) +
                     (JPath(".foo.bar") -> Set.empty[UserMetadata]) +
                     (JPath(".foo.bat") -> Set.empty[UserMetadata]) +
                     (JPath(".foo.baz") -> Set.empty[UserMetadata])


      val msg = EventMessage(EventId(0,0), Event(Path("/a/b"), "token", jval, metadata))

      val colDesc1 = ColumnDescriptor(Path("/a/b/"), CPath(".selector"), CString, Authorities(Set("token")))
      val colDesc2 = ColumnDescriptor(Path("/a/b/"), CPath(".foo.bar"), CLong, Authorities(Set("token")))
      val colDesc3 = ColumnDescriptor(Path("/a/b/"), CPath(".foo.bat"), CDouble, Authorities(Set("token")))
      val colDesc4 = ColumnDescriptor(Path("/a/b/"), CPath(".foo.baz"), CNum, Authorities(Set("token")))

      val actions = rt.routeEvent(msg)

      val expected = Seq(
        ProjectionData(toProjDesc(colDesc1 :: Nil), VectorCase[CValue](CString("Test")), List(Set.empty)),
        ProjectionData(toProjDesc(colDesc2 :: Nil), VectorCase[CValue](CLong(123)), List(Set.empty)),
        ProjectionData(toProjDesc(colDesc3 :: Nil), VectorCase[CValue](CDouble(456.78)), List(Set.empty)),
        ProjectionData(toProjDesc(colDesc4 :: Nil), VectorCase[CValue](CNum(BigDecimal("91011.12E+1314"))), List(Set.empty))
      )

      actions must containAllOf(expected).only
      
    }
  }
}
