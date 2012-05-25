package com.precog.yggdrasil
package actor

import com.precog.common._

import blueeyes.json.JsonAST._
import blueeyes.json.JPath

import org.specs2.mutable._
import org.specs2.matcher.{Matcher, MatchResult, Expectable}

import scala.collection.immutable.ListMap

class RoutingTableSpec extends Specification {
  
  "SingleColumnProjectionRoutingTable" should {

    def toProjDesc(colDescs: List[ColumnDescriptor]) = 
      ProjectionDescriptor( colDescs.foldRight( ListMap[ColumnDescriptor, Int]() ) { (el, acc) => acc + (el->0) }, colDescs.map { (_, ById) } ).toOption.get

    "project an event with one property to a single projection action" in {
      val rt = new SingleColumnProjectionRoutingTable
      
      val jval = JObject(
        JField("selector", JString("Test")) :: Nil 
      )

      val metadata = Map[JPath, Set[UserMetadata]]() +
                     (JPath(".selector") -> Set.empty[UserMetadata])
      
      val msg = EventMessage(EventId(0,0), Event(Path("/a/b"), "token", jval, metadata))
      
      val colDesc = ColumnDescriptor(Path("/a/b/"),JPath(".selector"), CStringArbitrary, Authorities(Set("token")))

      val actions = rt.route(msg)

      val expected = Seq(ProjectionData(toProjDesc(colDesc :: Nil), List[CValue](CString("Test")), List(Set.empty)))

      actions must containAllOf(expected).only
    }

    "project an event with n properties to n projection actions" in {
      val rt = new SingleColumnProjectionRoutingTable

      val jval = JObject(
        JField("selector", JString("Test")) ::
        JField("foo", JObject( JField("bar", JInt(123)) :: Nil )) :: Nil
      )

      val metadata = Map[JPath, Set[UserMetadata]]() +
                     (JPath(".selector") -> Set.empty[UserMetadata]) +
                     (JPath(".foo.bar") -> Set.empty[UserMetadata])

      val msg = EventMessage(EventId(0,0), Event(Path("/a/b"), "token", jval, metadata))

      val colDesc1 = ColumnDescriptor(Path("/a/b/"),JPath(".selector"), CStringArbitrary, Authorities(Set("token")))
      val colDesc2 = ColumnDescriptor(Path("/a/b/"),JPath(".foo.bar"), CInt, Authorities(Set("token")))

      val actions = rt.route(msg)

      val expected = Seq(
        ProjectionData(toProjDesc(colDesc1 :: Nil), List[CValue](CString("Test")), List(Set.empty)),
        ProjectionData(toProjDesc(colDesc2 :: Nil), List[CValue](CInt(123)), List(Set.empty))
      )

      actions must containAllOf(expected).only
      
    }
  }
}
