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
    
    "refrain from sorting when sets are already aligned in match" in {
      val line = Line(0, "")
      
      val left = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
      val right = Root(line, PushNum("42"))
      
      val input = Join(line, Map2Match(Or), Join(line, Map2Cross(Eq), left, right), left)
      val expected = Join(line, Map2Match(Or), Join(line, Map2CrossLeft(Eq), left, right), left)
      
      orderCrosses(input) mustEqual expected
    }
    
    "refrain from sorting when sets are already aligned in filter" in {
      val line = Line(0, "")
      
      val left = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
      val right = Root(line, PushNum("42"))
      
      val input = Filter(line, None, None, Join(line, Map2Cross(Eq), left, right), left)
      val expected = Filter(line, None, None, Join(line, Map2CrossLeft(Eq), left, right), left)
      
      orderCrosses(input) mustEqual expected
    }
  }
}
