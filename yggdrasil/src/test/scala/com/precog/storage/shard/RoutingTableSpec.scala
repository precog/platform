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

    def toProjDesc(colDescs: List[ColumnDescriptor]) = 
      ProjectionDescriptor( colDescs.foldRight( ListMap[ColumnDescriptor, Int]() ) { (el, acc) => acc + (el->0) }, colDescs.map { (_, ById) } ).toOption.get

    "project an empty event to an empty set of projection actions" in {
      val rt = SingleColumnProjectionRoutingTable 

      rt.route(EventData(0, Set.empty)) must_== Set.empty
    }

    "project an event with one property to a single projection action" in {
      val rt = SingleColumnProjectionRoutingTable 

      val colDesc = ColumnDescriptor(Path("/a/b/"),JPath(".selector"), SLong, Authorities(Set()))

      val event = EventData(0, Set(ColumnData(colDesc, CString("Test"), Set.empty)))

      val actions = rt.route(event)

      val expected : Set[ProjectionData] = 
        Set(ProjectionData(toProjDesc(colDesc :: Nil), VectorCase(event.identity),List[CValue](CString("Test")), List(Set.empty)))

      actions must_== expected 
    }

    "project an event with n properties to n projection actions" in {
      val rt = SingleColumnProjectionRoutingTable 

      val colDesc1 = ColumnDescriptor(Path("/a/b/"),JPath(".selector"), SLong, Authorities(Set()))
      val colDesc2 = ColumnDescriptor(Path("/a/b/"),JPath(".selector.foo"), SLong, Authorities(Set()))

      val event = EventData(0, Set(ColumnData(colDesc1, CString("Test"), Set.empty),
                                   ColumnData(colDesc2, CInt(1), Set.empty)))

      val actions = rt.route(event)

      val expected : Set[ProjectionData] = Set(
          ProjectionData(toProjDesc(colDesc1 :: Nil), VectorCase(event.identity),List[CValue](CString("Test")), List(Set.empty)),
          ProjectionData(toProjDesc(colDesc2 :: Nil), VectorCase(event.identity),List[CValue](CInt(1)), List(Set.empty))
      )

      actions must_== expected
    }
  }
}
