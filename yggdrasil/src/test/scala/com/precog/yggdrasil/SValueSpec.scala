package com.precog.yggdrasil

import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import com.precog.common.VectorCase

import org.specs2.mutable.Specification
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary._

class SValueSpec extends Specification {
  "set" should {
    "set properties on an object" in {
      SObject(Map()).set(JPath(".foo.bar"), CString("baz")) must beSome(SObject(Map("foo" -> SObject(Map("bar" -> SString("baz"))))))
    }

    "set array indices" in {
      SObject(Map()).set(JPath(".foo[1].bar"), CString("baz")) must beSome(SObject(Map("foo" -> SArray(Vector(SNull, SObject(Map("bar" -> SString("baz"))))))))
    }

    "return None for a primitive" in {
      STrue.set(JPath(".foo.bar"), CString("hi")) must beNone
    }
  }

  "structure" should {
    "return correct sequence for an array" in {
      SArray(Vector(SBoolean(true))).structure must_== Seq((JPath("[0]"), CBoolean))
    }
  }
}

// vim: set ts=4 sw=4 et:
