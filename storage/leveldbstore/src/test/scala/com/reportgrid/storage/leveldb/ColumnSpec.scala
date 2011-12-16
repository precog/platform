package com.reportgrid.storage.leveldb

import org.scalacheck.{Arbitrary,Gen}
import org.specs2.ScalaCheck
import org.specs2.matcher.ThrownMessages
import org.specs2.mutable.Specification
import Bijection._

class ColumnSpec extends Specification with ScalaCheck with ThrownMessages {
//  "Columns" should {
//    val c = new Column("test", "/tmp")
//
//    "Properly persist ID lists" in {
//      check { (v : BigDecimal, ids : List[Long]) =>
//        ids.foreach(c.insert(_, v.underlying))
//        
//        val resultIds = c.getIds(v.underlying)
//
//        resultIds.size must_== ids.size
//      }
//    }
//  }
}
