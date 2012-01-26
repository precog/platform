package com.reportgrid.yggdrasil
package shard

import com.reportgrid.common._

import com.reportgrid.analytics.Path

import blueeyes.json.JsonAST._
import blueeyes.json.JPath

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, MatchResult, Expectable}

import scala.collection.immutable.ListMap

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
        (ColumnDescriptor(Path("/a/b/"),JPath(".selector"), SLong, Ownership(Set())), JString("Test"))
      )

      val actions = rt.route(event.toSet)

      val expected : Set[ProjectionDescriptor] = sys.error("todo")//Set( (ProjectionDescriptor(event.map( _._1 ), Set()), event.map( _._2 )) )

      actions must_== expected 
    }.pendingUntilFixed

    "project an event with n properties to n projection actions" in {
      val rt = new SingleColumnProjectionRoutingTable 

      val event: List[(ColumnDescriptor, JValue)] = List(
        (ColumnDescriptor(Path("/a/b/"),JPath(".selector"), SLong, Ownership(Set())), JString("Test")),
        (ColumnDescriptor(Path("/a/b/"),JPath(".selector.foo"), SLong, Ownership(Set())), JInt(1))
      )

      val actions = rt.route(event.toSet)

      val qss = event.map( _._1 )
      val vals = event.map( _._2 )

      val expected : Set[ProjectionDescriptor] = sys.error("todo")
        Set( (ProjectionDescriptor(ListMap() + (qss(0) -> 0), List() :+ (qss(0) -> ById) ), List(vals(0))),
             (ProjectionDescriptor(ListMap() + (qss(1) -> 0), List() :+ (qss(0) -> ById) ), List(vals(1))) )

      actions must_== expected 
    }.pendingUntilFixed
  }
}
