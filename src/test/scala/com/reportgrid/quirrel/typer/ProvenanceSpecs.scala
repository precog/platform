package com.reportgrid.quirrel
package typer

import edu.uwm.cs.gll.LineStream
import org.specs2.mutable.Specification
import parser._

object ProvenanceSpecs extends Specification with Parser with StubPasses with ProvenanceChecker {
  override type Error = String
  
  "provenance checking" should {
    "reject addition on different datasets" in {
      val tree = parse("dataset(//a) + dataset(//b)")
      tree.provenance mustEqual NullProvenance
      tree.errors mustEqual Set("cannot perform operation on unrelated sets")
    }
  }
  
  def parse(str: String): Expr = parse(LineStream(str))
}
