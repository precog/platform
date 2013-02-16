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

    def toProjDesc(colDescs: List[ColumnRef]) = ProjectionDescriptor(1, colDescs)

    "project an event with one property to a single projection action" in {
      val rt = new SingleColumnProjectionRoutingTable
      
      val jval = JObject(
        JField("selector", JString("Test")) :: Nil 
      )

      val eventId = EventId(0, 0)
      val msg = IngestMessage("apiKey", Path("/a/b"), "someOwner", Vector(IngestRecord(eventId, jval)), None)
      
      val colDesc = ColumnRef(Path("/a/b/"), CPath(".selector"), CString, Authorities(Set("someOwner")))

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

      val colDesc1 = ColumnRef(Path("/a/b/"), CPath(".selector"), CString, Authorities(Set("someOwner")))
      val colDesc2 = ColumnRef(Path("/a/b/"), CPath(".foo.bar"), CLong, Authorities(Set("someOwner")))
      val colDesc3 = ColumnRef(Path("/a/b/"), CPath(".foo.bat"), CDouble, Authorities(Set("someOwner")))
      val colDesc4 = ColumnRef(Path("/a/b/"), CPath(".foo.baz"), CNum, Authorities(Set("someOwner")))

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
