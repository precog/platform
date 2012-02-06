package com.precog.daze

import org.specs2.mutable._

object CrossOrderingSpecs extends Specification with CrossOrdering {
  import instructions._
  import dag._
  
  "cross ordering" should {
    "order in the appropriate direction when one side is singleton" >> {
      "left" >> {
        val line = Line(0, "")
        
        val left = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
        val right = Root(line, PushNum("42"))
        
        val input = Join(line, Map2Cross(Eq), left, right)
        val expected = Join(line, Map2CrossLeft(Eq), left, right)
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(0, "")
        
        val left = Root(line, PushNum("42"))
        val right = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
        
        val input = Join(line, Map2Cross(Eq), left, right)
        val expected = Join(line, Map2CrossRight(Eq), left, right)
        
        orderCrosses(input) mustEqual expected
      }
    }
    
    "insert index 0 sorts on dynamic matching filter" in {
      val line = Line(0, "")
      val split = dag.Split(line, Root(line, PushNum("42")), Root(line, PushNum("24")))

      val left = split
      val right = Join(line, Map2Cross(Eq), split, Root(line, PushNum("9")))

      val input = Filter(line, None, None, left, right)

      left.provenance must beLike { case Vector(DynamicProvenance(_)) => ok }
      val num = left.provenance.head.asInstanceOf[DynamicProvenance].id

      right.provenance mustEqual Vector(DynamicProvenance(num))
      input.provenance mustEqual Vector(DynamicProvenance(num))

      val expected = Filter(line, None, None,
        Sort(left, Vector(0)),
        Sort(Join(line, Map2CrossLeft(Eq), split, Root(line, PushNum("9"))), Vector(0)))

      orderCrosses(input) mustEqual expected
    }
  }
}
