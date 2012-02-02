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
package shard

import com.precog.common._

import com.precog.analytics.Path

import blueeyes.json.JsonAST._
import blueeyes.json.JPath

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, MatchResult, Expectable}

import scala.collection.immutable.ListMap

class RoutingTableSpec extends Specification {
  
  "SingleColumnProjectionRoutingTable" should {

    "project an empty event to an empty set of projection actions" in {
      val rt = SingleColumnProjectionRoutingTable 

      rt.route(EventData(0, Set.empty)) must_== Set.empty
    }

    "project an event with one property to a single projection action" in {
      val rt = SingleColumnProjectionRoutingTable 

      val colDesc = ColumnDescriptor(Path("/a/b/"),JPath(".selector"), SLong, Ownership(Set()))

      val event = EventData(0, Set(ColumnData(colDesc, CString("Test"), Set.empty)))

      val actions = rt.route(event)

      val expected : Set[ProjectionDescriptor] = sys.error("todo")//Set( (ProjectionDescriptor(event.map( _._1 ), Set()), event.map( _._2 )) )

      actions must_== expected 
    }.pendingUntilFixed

    "project an event with n properties to n projection actions" in {
      val rt = SingleColumnProjectionRoutingTable 

      val colDesc1 = ColumnDescriptor(Path("/a/b/"),JPath(".selector"), SLong, Ownership(Set()))
      val colDesc2 = ColumnDescriptor(Path("/a/b/"),JPath(".selector.foo"), SLong, Ownership(Set()))

      val event = EventData(0, Set(ColumnData(colDesc1, CString("Test"), Set.empty),
                                   ColumnData(colDesc2, CInt(1), Set.empty)))

      val actions = rt.route(event)

      val expected : Set[ProjectionDescriptor] = sys.error("todo")

      actions must_== expected
    }.pendingUntilFixed
  }
}
