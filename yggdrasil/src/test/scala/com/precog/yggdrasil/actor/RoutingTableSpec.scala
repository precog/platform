package com.precog.yggdrasil
package actor

import com.precog.common._
import com.precog.common.json._

import blueeyes.json._

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
      
      val msg = EventMessage(EventId(0,0), Event("apiKey", Path("/a/b"), Some("someOwner"), jval, metadata))
      
      val colDesc = ColumnDescriptor(Path("/a/b/"), CPath(".selector"), CString, Authorities(Set("someOwner")))

      val actions = rt.routeEvent(msg)

      val expected = Seq(ProjectionData(toProjDesc(colDesc :: Nil), List[CValue](CString("Test")), List(Set.empty)))

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

      val metadata = Map[JPath, Set[UserMetadata]]() +
                     (JPath(".selector") -> Set.empty[UserMetadata]) +
                     (JPath(".foo.bar") -> Set.empty[UserMetadata]) +
                     (JPath(".foo.bat") -> Set.empty[UserMetadata]) +
                     (JPath(".foo.baz") -> Set.empty[UserMetadata])


      val msg = EventMessage(EventId(0,0), Event("apiKey", Path("/a/b"), Some("someOwner"), jval, metadata))

      val colDesc1 = ColumnDescriptor(Path("/a/b/"), CPath(".selector"), CString, Authorities(Set("someOwner")))
      val colDesc2 = ColumnDescriptor(Path("/a/b/"), CPath(".foo.bar"), CLong, Authorities(Set("someOwner")))
      val colDesc3 = ColumnDescriptor(Path("/a/b/"), CPath(".foo.bat"), CDouble, Authorities(Set("someOwner")))
      val colDesc4 = ColumnDescriptor(Path("/a/b/"), CPath(".foo.baz"), CNum, Authorities(Set("someOwner")))

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
