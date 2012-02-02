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
