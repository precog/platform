package com.reportgrid.quirrel
package parser

import edu.uwm.cs.gll.LineStream
import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.mutable._

object ParserSpecs extends Specification with ScalaCheck {
  "parsing a trivial expression" should {
    "parse" in {
      val parser = new Parser {
        def input = LineStream("x('a) := 1 1")
      }

      parser.root mustEqual null
    }
  }
}
