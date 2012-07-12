package com.precog
package daze

import bytecode.RandomLibrary
import org.specs2.mutable._

object CrossOrderingSpecs extends Specification with CrossOrdering with RandomLibrary {
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
      
      val input = Filter(line, None, Join(line, Map2Cross(Eq), left, right), left)
      val expected = Filter(line, None, Join(line, Map2CrossLeft(Eq), left, right), left)
      
      orderCrosses(input) mustEqual expected
    }
    
    // this will eventually be a re-order cross test case
    "insert sorts for match on out-of-order operand set" >> {
      "left" >> {
        val line = Line(0, "")
        
        val left = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
        val right = Join(line, Map2CrossRight(Add),
          left,
          dag.LoadLocal(line, None, Root(line, PushString("/bar")), Het))
        
        val input = Join(line, Map2Match(Or), left, right)
        val expected = Join(line, Map2Match(Or), left, Sort(right, Vector(1)))
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(0, "")
        
        val right = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
        val left = Join(line, Map2CrossRight(Add),
          right,
          dag.LoadLocal(line, None, Root(line, PushString("/bar")), Het))
        
        val input = Join(line, Map2Match(Or), left, right)
        val expected = Join(line, Map2Match(Or), Sort(left, Vector(1)), right)
        
        orderCrosses(input) mustEqual expected
      }
      
      "both" >> {
        val line = Line(0, "")
        
        val foo = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
        val bar = dag.LoadLocal(line, None, Root(line, PushString("/bar")), Het)
        val baz = dag.LoadLocal(line, None, Root(line, PushString("/baz")), Het)
        
        val left = Join(line, Map2CrossRight(Add), foo, bar)
        val right = Join(line, Map2CrossRight(Add), foo, baz)
        
        val input = Join(line, Map2Match(Or), left, right)
        val expected = Join(line, Map2Match(Or), Sort(left, Vector(1)), Sort(right, Vector(1)))
        
        orderCrosses(input) mustEqual expected
      }
      
      "random-case-without-a-label" >> {
        val line = Line(0, "")
        
        val numbers = dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers")), Het)
        val numbers3 = dag.LoadLocal(line, None, Root(line, PushString("/hom/numbers3")), Het)
        
        val input = Join(line, Map2Match(And),
          Join(line, Map2Cross(And),
            Join(line, Map2Match(Eq), numbers, numbers),
            Join(line, Map2Match(Eq), numbers3, numbers3)),
          Join(line, Map2Match(Eq), numbers3, numbers3))
        
        val expected = Join(line, Map2Match(And),
          Sort(
            Join(line, Map2CrossLeft(And),
              Join(line, Map2Match(Eq), numbers, numbers),
              Memoize(Join(line, Map2Match(Eq), numbers3, numbers3), 100)),
            Vector(1)),
          Join(line, Map2Match(Eq), numbers3, numbers3))
            
        orderCrosses(input) mustEqual expected
      }
    }
    
    // this will eventually be a re-order cross test case
    "insert sorts for filter on out-of-order operand set" >> {
      "left" >> {
        val line = Line(0, "")
        
        val left = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
        val right = Join(line, Map2CrossRight(Add),
          left,
          dag.LoadLocal(line, None, Root(line, PushString("/bar")), Het))
        
        val input = Filter(line, None, left, right)
        val expected = Filter(line, None, left, Sort(right, Vector(1)))
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(0, "")
        
        val right = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
        val left = Join(line, Map2CrossRight(Add),
          right,
          dag.LoadLocal(line, None, Root(line, PushString("/bar")), Het))
        
        val input = Filter(line, None, left, right)
        val expected = Filter(line, None, Sort(left, Vector(1)), right)
        
        orderCrosses(input) mustEqual expected
      }
      
      "both" >> {
        val line = Line(0, "")
        
        val foo = dag.LoadLocal(line, None, Root(line, PushString("/foo")), Het)
        val bar = dag.LoadLocal(line, None, Root(line, PushString("/bar")), Het)
        val baz = dag.LoadLocal(line, None, Root(line, PushString("/baz")), Het)
        
        val left = Join(line, Map2CrossRight(Add), foo, bar)
        val right = Join(line, Map2CrossRight(Add), foo, baz)
        
        val input = Filter(line, None, left, right)
        val expected = Filter(line, None, Sort(left, Vector(1)), Sort(right, Vector(1)))
        
        orderCrosses(input) mustEqual expected
      }
    }
  }
}
