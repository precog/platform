package com.precog.yggdrasil

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import org.specs2.mutable.Specification

class SValueSpec extends Specification {
  "set" should {
    "set properties on an object" in {
      SObject(Map()).set(JPath(".foo.bar"), CString("baz")) must beSome(SObject(Map("foo" -> SObject(Map("bar" -> SString("baz"))))))
    }

    "set array indices" in {
      SObject(Map()).set(JPath(".foo[1].bar"), CString("baz")) must beSome(SObject(Map("foo" -> SArray(Vector(SNull, SObject(Map("bar" -> SString("baz"))))))))
    }

    "return None for a primitive" in {
      SLong(1).set(JPath(".foo.bar"), CString("hi")) must beNone
    }
  }
}

// vim: set ts=4 sw=4 et:
