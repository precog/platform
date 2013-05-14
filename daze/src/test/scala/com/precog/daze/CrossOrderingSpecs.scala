package com.precog
package daze

import com.precog.common._
import bytecode._
import org.specs2.mutable._
import com.precog.yggdrasil._

object CrossOrderingSpecs extends Specification with CrossOrdering with FNDummyModule {
  import instructions._
  import dag._
  import TableModule.CrossOrder._

  type Lib = RandomLibrary
  object library extends RandomLibrary
  
  "cross ordering" should {
    "order in the appropriate direction when one side is singleton" >> {
      "left" >> {
        val line = Line(1, 1, "")
        
        val left = dag.LoadLocal(Const(CString("/foo"))(line))(line)
        val right = Const(CLong(42))(line)
        
        val input = Join(Eq, Cross(None), left, right)(line)
        val expected = Join(Eq, Cross(Some(CrossLeft)), left, right)(line)
        
        orderCrosses(input) mustEqual expected
      }
      
      "right" >> {
        val line = Line(1, 1, "")
        
        val left = Const(CLong(42))(line)
        val right = dag.LoadLocal(Const(CString("/foo"))(line))(line)
        
        val input = Join(Eq, Cross(None), left, right)(line)
        val expected = Join(Eq, Cross(Some(CrossRight)), left, right)(line)
        
        orderCrosses(input) mustEqual expected
      }
    }
    
    "refrain from sorting when sets are already aligned in match" in {
      val line = Line(1, 1, "")
      
      val left = dag.LoadLocal(Const(CString("/foo"))(line))(line)
      val right = Const(CLong(42))(line)
      
      val input = Join(Or, IdentitySort, Join(Eq, Cross(None), left, right)(line), left)(line)
      val expected = Join(Or, IdentitySort, Join(Eq, Cross(Some(CrossLeft)), left, right)(line), left)(line)
      
      orderCrosses(input) mustEqual expected
    }
    
    "refrain from sorting when sets are already aligned in filter" in {
      val line = Line(1, 1, "")
      
      val left = dag.LoadLocal(Const(CString("/foo"))(line))(line)
      val right = Const(CLong(42))(line)
      
      val input = Filter(IdentitySort, Join(Eq, Cross(None), left, right)(line), left)(line)
      val expected = Filter(IdentitySort, Join(Eq, Cross(Some(CrossLeft)), left, right)(line), left)(line)
      
      orderCrosses(input) mustEqual expected
    }

    "memoize RHS of cross when it is not a forcing point" in {
      val line = Line(1, 1, "")
      
      val foo = dag.LoadLocal(Const(CString("/foo"))(line), JTextT)(line)
      val bar = dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line)
      
      val barAdd = Join(Add, IdentitySort, bar, bar)(line)
      
      val input = Join(Add, Cross(None), foo, barAdd)(line)
      
      val expected = Join(Add, Cross(None), foo, Memoize(barAdd, 100))(line)
      
      orderCrosses(input) mustEqual expected
    }
    
    "refrain from memoizing RHS of cross when it is a forcing point" in {
      val line = Line(1, 1, "")
      
      val foo = dag.LoadLocal(Const(CString("/foo"))(line), JTextT)(line)
      val bar = dag.LoadLocal(Const(CString("/bar"))(line), JTextT)(line)
      
      val input = Join(Add, Cross(None), foo, bar)(line)
      
      orderCrosses(input) mustEqual input
    }
    
    "refrain from resorting by identity when cogrouping after an ordered cross" in {
      val line = Line(1, 1, "")
      
      val foo = dag.LoadLocal(Const(CString("/foo"))(line), JTextT)(line)
      
      val input =
        Join(Add, IdentitySort,
          Join(Add, Cross(Some(CrossLeft)),
            foo,
            Const(CLong(42))(line))(line),
          foo)(line)
          
      orderCrosses(input) mustEqual input
    }
    
    "refrain from resorting by value when cogrouping after an ordered cross" in {
      val line = Line(1, 1, "")
      
      val foo = dag.LoadLocal(Const(CString("/foo"))(line), JTextT)(line)
      
      val input =
        Join(Add, ValueSort(0),
          Join(Add, Cross(Some(CrossLeft)),
            AddSortKey(foo, "a", "b", 0),
            Const(CLong(42))(line))(line),
          AddSortKey(foo, "a", "b", 0))(line)
          
      orderCrosses(input) mustEqual input
    }
  }
}
