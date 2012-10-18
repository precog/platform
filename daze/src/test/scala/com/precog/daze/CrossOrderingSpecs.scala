package com.precog
package daze

import bytecode._
import org.specs2.mutable._
import com.precog.yggdrasil._

object CrossOrderingSpecs extends Specification with CrossOrdering with RandomLibrary {
  import instructions._
  import dag._
  
  "cross ordering" should {
    "order in the appropriate direction when one side is singleton" >> {
      "left" >> {
        val line = Line(0, "")
        
        val left = dag.LoadLocal(line, Root(line, CString("/foo")))
        val right = Root(line, CLong(42))
        
        val input = Join(line, Eq, CrossRightSort, left, right)
        val expected = Join(line, Eq, CrossLeftSort, left, right)
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(0, "")
        
        val left = Root(line, CLong(42))
        val right = dag.LoadLocal(line, Root(line, CString("/foo")))
        
        val input = Join(line, Eq, CrossLeftSort, left, right)
        val expected = Join(line, Eq, CrossRightSort, left, right)
        
        orderCrosses(input) mustEqual expected
      }
    }
    
    "refrain from sorting when sets are already aligned in match" in {
      val line = Line(0, "")
      
      val left = dag.LoadLocal(line, Root(line, CString("/foo")))
      val right = Root(line, CLong(42))
      
      val input = Join(line, Or, IdentitySort, Join(line, Eq, CrossRightSort, left, right), left)
      val expected = Join(line, Or, IdentitySort, Join(line, Eq, CrossLeftSort, left, right), left)
      
      orderCrosses(input) mustEqual expected
    }
    
    "refrain from sorting when sets are already aligned in filter" in {
      val line = Line(0, "")
      
      val left = dag.LoadLocal(line, Root(line, CString("/foo")))
      val right = Root(line, CLong(42))
      
      val input = Filter(line, IdentitySort, Join(line, Eq, CrossRightSort, left, right), left)
      val expected = Filter(line, IdentitySort, Join(line, Eq, CrossLeftSort, left, right), left)
      
      orderCrosses(input) mustEqual expected
    }

    // this will eventually be a re-order cross test case
    "insert sorts for match on out-of-order operand set" >> {
      "left" >> {
        val line = Line(0, "")
        
        val left = dag.LoadLocal(line, Root(line, CString("/foo")), JTextT)
        val right = Join(line, Add, CrossRightSort,
          dag.LoadLocal(line, Root(line, CString("/bar")), JTextT),
          left)
        
        val input = Join(line, Or, IdentitySort, left, right)
        
        val expectedRight = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, CString("/bar")), JTextT),
          left)

        val expected = Join(line, Or, IdentitySort, left, Sort(expectedRight, Vector(1)))
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(0, "")
        
        val right = dag.LoadLocal(line, Root(line, CString("/foo")), JTextT)
        val left = Join(line, Add, CrossRightSort,
          dag.LoadLocal(line, Root(line, CString("/bar")), JTextT),
          right)
        
        val input = Join(line, Or, IdentitySort, left, right)
        
        val expectedLeft = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, CString("/bar")), JTextT),
          right)

        val expected = Join(line, Or, IdentitySort, Sort(expectedLeft, Vector(1)), right)
        
        orderCrosses(input) mustEqual expected
      }
      
      "both" >> {
        val line = Line(0, "")
        
        val foo = dag.LoadLocal(line, Root(line, CString("/foo")), JTextT)
        val bar = dag.LoadLocal(line, Root(line, CString("/bar")), JTextT)
        val baz = dag.LoadLocal(line, Root(line, CString("/baz")), JTextT)
        
        val left = Join(line, Add, CrossRightSort, bar, foo)
        val right = Join(line, Add, CrossRightSort, baz, foo)
        
        val expectedLeft = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, CString("/bar")), JTextT),
          foo)

        val expectedRight = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, CString("/baz")), JTextT),
          foo)

        val input = Join(line, Or, IdentitySort, left, right)
        val expected = Join(line, Or, IdentitySort, Sort(expectedLeft, Vector(1)), Sort(expectedRight, Vector(1)))
        
        orderCrosses(input) mustEqual expected
      }
      
      "random-case-without-a-label" >> {
        val line = Line(0, "")
        
        val numbers = dag.LoadLocal(line, Root(line, CString("/hom/numbers")), JTextT)
        val numbers3 = dag.LoadLocal(line, Root(line, CString("/hom/numbers3")), JTextT)
        
        val input = Join(line, Add, IdentitySort,
          Join(line, Add, CrossRightSort,
            Join(line, Eq, IdentitySort, numbers, numbers),
            Join(line, Eq, IdentitySort, numbers3, numbers3)),
          Join(line, Eq, IdentitySort, numbers3, numbers3))
        
        val expected = Join(line, Add, IdentitySort,
          Sort(
            Join(line, Add, CrossLeftSort,
              Join(line, Eq, IdentitySort, numbers, numbers),
              Memoize(Join(line, Eq, IdentitySort, numbers3, numbers3), 100)),
            Vector(1)),
          Join(line, Eq, IdentitySort, numbers3, numbers3))
            
        orderCrosses(input) mustEqual expected
      }
    }

    // this will eventually be a re-order cross test case
    "insert sorts for filter on out-of-order operand set" >> {
      "left" >> {
        val line = Line(0, "")
        
        val left = dag.LoadLocal(line, Root(line, CString("/foo")), JTextT)
        val right = Join(line, Add, CrossRightSort,
          dag.LoadLocal(line, Root(line, CString("/bar")), JTextT),
          left)
        
        val input = Filter(line, IdentitySort, left, right)
        
        val expectedRight = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, CString("/bar")), JTextT),
          left)

        val expected = Filter(line, IdentitySort, left, Sort(expectedRight, Vector(1)))
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(0, "")
        
        val right = dag.LoadLocal(line, Root(line, CString("/foo")), JTextT)
        val left = Join(line, Add, CrossRightSort,
          dag.LoadLocal(line, Root(line, CString("/bar")), JTextT),
          right)
        
        val input = Filter(line, IdentitySort, left, right)
        
        val expectedLeft = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, CString("/bar")), JTextT),
          right)
        
        val expected = Filter(line, IdentitySort, Sort(expectedLeft, Vector(1)), right)
        
        orderCrosses(input) mustEqual expected
      }
      
      "both" >> {
        val line = Line(0, "")
        
        val foo = dag.LoadLocal(line, Root(line, CString("/foo")), JTextT)
        val bar = dag.LoadLocal(line, Root(line, CString("/bar")), JTextT)
        val baz = dag.LoadLocal(line, Root(line, CString("/baz")), JTextT)
        
        val left = Join(line, Add, CrossRightSort, bar, foo)
        val right = Join(line, Add, CrossRightSort, baz, foo)
        
        val expectedLeft = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, CString("/bar")), JTextT),
          foo)

        val expectedRight = Join(line, Add, CrossLeftSort,
          dag.LoadLocal(line, Root(line, CString("/baz")), JTextT),
          foo)

        val input = Filter(line, IdentitySort, left, right)
        val expected = Filter(line, IdentitySort, Sort(expectedLeft, Vector(1)), Sort(expectedRight, Vector(1)))
        
        orderCrosses(input) mustEqual expected
      }
    }
    
    "memoize RHS of cross when it is not a forcing point" in {
      val line = Line(0, "")
      
      val foo = dag.LoadLocal(line, Root(line, CString("/foo")), JTextT)
      val bar = dag.LoadLocal(line, Root(line, CString("/bar")), JTextT)
      
      val barAdd = Join(line, Add, IdentitySort, bar, bar)
      
      val input = Join(line, Add, CrossLeftSort, foo, barAdd)
      
      val expected = Join(line, Add, CrossLeftSort, foo, Memoize(barAdd, 100))
      
      orderCrosses(input) mustEqual expected
    }
    
    "refrain from memoizing RHS of cross when it is a forcing point" in {
      val line = Line(0, "")
      
      val foo = dag.LoadLocal(line, Root(line, CString("/foo")), JTextT)
      val bar = dag.LoadLocal(line, Root(line, CString("/bar")), JTextT)
      
      val input = Join(line, Add, CrossLeftSort, foo, bar)
      
      orderCrosses(input) mustEqual input
    }
  }
}
