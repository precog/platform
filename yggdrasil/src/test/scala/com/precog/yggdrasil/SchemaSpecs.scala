package com.precog.yggdrasil

import com.precog.bytecode._

import com.precog.common._

import org.specs2.mutable.Specification

class SchemaSpec extends Specification {
  "cpath" should {
    "return the correct sequence of CPath" in {
      val jtype = JObjectFixedT(Map(
        "foo" -> JNumberT, 
        "bar" -> JArrayFixedT(Map(
          0 -> JBooleanT, 
          1 -> JObjectFixedT(Map(
            "baz" -> JArrayHomogeneousT(JNullT))),
          2 -> JTextT))))

      val result = Schema.cpath(jtype)
      val expected = Seq(
        CPath(CPathField("foo")),
        CPath(CPathField("bar"), CPathIndex(0)),
        CPath(CPathField("bar"), CPathIndex(1), CPathField("baz"), CPathArray),
        CPath(CPathField("bar"), CPathIndex(2))) sorted
        
      result mustEqual expected
    }
  }
}
