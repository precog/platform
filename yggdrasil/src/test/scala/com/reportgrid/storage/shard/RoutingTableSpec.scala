package com.reportgrid.yggdrasil
package shard

import com.reportgrid.common._

import com.reportgrid.analytics.Path

import blueeyes.json.JsonAST._
import blueeyes.json.JPath

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, MatchResult, Expectable}

class RoutingTableSpec extends Specification {
  
  "SingleColumnProjectionRoutingTable" should {

    "project an empty event to an empty set of projection actions" in {
      val rt = new SingleColumnProjectionRoutingTable 

      val event: Set[(ColumnDescriptor, JValue)] = Set.empty

      val actions = rt.route(event)

      actions must_== Set.empty
    }

    "project an event with one property to a single projection action" in {
      val rt = new SingleColumnProjectionRoutingTable 

      val event: List[(ColumnDescriptor, JValue)] = List(
        (ColumnDescriptor(Path("/a/b/"),JPath(".selector"), SLong), JString("Test"))
      )

      val actions = rt.route(event.toSet)

      val expected : Set[ProjectionDescriptor] = sys.error("todo")//Set( (ProjectionDescriptor(event.map( _._1 ), Set()), event.map( _._2 )) )

      actions must_== expected 
    }.pendingUntilFixed

    "project an event with n properties to n projection actions" in {
      val rt = new SingleColumnProjectionRoutingTable 

      val event: List[(ColumnDescriptor, JValue)] = List(
        (ColumnDescriptor(Path("/a/b/"),JPath(".selector"), SLong), JString("Test")),
        (ColumnDescriptor(Path("/a/b/"),JPath(".selector.foo"), SLong), JInt(1))
      )

      val actions = rt.route(event.toSet)

      val qss = event.map( _._1 )
      val vals = event.map( _._2 )

      val expected : Set[ProjectionDescriptor] = sys.error("todo")
        //Set( (ProjectionDescriptor(List(qss(0)), Set()), List(vals(0))),
        //                  (ProjectionDescriptor(List(qss(1)), Set()), List(vals(1))) )

      actions must_== expected 
    }.pendingUntilFixed
  }
}
