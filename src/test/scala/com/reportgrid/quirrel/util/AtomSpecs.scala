package com.reportgrid.quirrel
package util

import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.mutable._

object AtomSpecs extends Specification with ScalaCheck {
  import Atom._
  import Prop._
  
  "reference atoms" should {
    "store and return value after update" in check { i: Int =>
      val a = atom[Int]
      a() = i
      a() mustEqual i
      a() mustEqual i
    }
    
    "throw exception for unset value" in {
      val a = atom[Int]
      a() must throwA[RuntimeException]
    }
    
    "store last update on multiple sets" in {
      val a = atom[Int]
      a() = 42
      a() = 12
      a() mustEqual 12
    }
    
    "silently fail on multiply-set value after force" in {
      val a = atom[Int]
      a() = 42
      a()      // force
      
      (a() = 12) must not(throwA[RuntimeException])
      a() mustEqual 42
    }
    
    "self-populate once on access" in check { i: Int =>
      var count = 0
      lazy val a: Atom[Int] = atom[Int] {
        count += 1
        a() = i
      }
      
      a() mustEqual i
      a() mustEqual i
      
      count mustEqual 1
    }
    
    "detect recursive self-population" in {
      lazy val a: Atom[Int] = atom[Int] {
        a() += 42
      }
      
      a() must throwA[RuntimeException]
    }
    
    "fail on invalid self-population" in {
      val a: Atom[Int] = atom[Int] {}   // null self-populator
      a() must throwA[RuntimeException]
    }
    
    // TODO spec multi-thread behavior
  }
  
  "aggregate atoms" should {
    "store all values and return" in check { xs: Set[Int] =>
      val a = new SetAtom[Int]
      xs foreach (a +=)
      
      a() mustEqual xs
      a() mustEqual xs
    }
    
    "concatenate values and return" in check { (i: Int, xs: Set[Int]) =>
      val a = new SetAtom[Int]
      a += i
      a ++= xs
      
      a() mustEqual (xs + i)
      a() mustEqual (xs + i)
    }
    
    "silently fail for mutation following force" in {
      val a = new SetAtom[Int]
      a += 42
      a() mustEqual Set(42)
      
      (a += 12) must not(throwA[RuntimeException])
      a() mustEqual Set(42)
    }
  }
}
