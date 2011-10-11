package com.reportgrid.quirrel
package parser

import edu.uwm.cs.gll.LineStream
import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.mutable._

object ParserSpecs extends Specification with ScalaCheck with Parser {
  
  "parameterized bind expression" should {
    "parse with a single parameter" in {
      parse(LineStream("x('a) := 1 1")) mustEqual Binding("x", Vector("'a"), NumLit("1"), NumLit("1"))
    }
  }
}
