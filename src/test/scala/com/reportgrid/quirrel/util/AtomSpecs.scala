package com.reportgrid.quirrel
package util

import org.scalacheck.Prop
import org.specs2.ScalaCheck
import org.specs2.mutable._

object AtomSpecs extends Specification with ScalaCheck {
  import Prop._
  
  "reference atoms" should {
    "store and return value after update" in check { i: Int =>
      val atom = new Atom[Int]
      atom() = i
      atom() mustEqual i
      atom() mustEqual i
    }
    
    "throw exception for unset value" in {
      val atom = new Atom[Int]
      atom() must throwA[RuntimeException]
    }
    
    "throw exception for multiply-set value" in {
      val atom = new Atom[Int]
      atom() = 42
      (atom() = 12) must throwA[RuntimeException]
      atom() mustEqual 42
    }
    
    "self-populate once on access" in check { i: Int =>
      var count = 0
      val atom = new Atom[Int] {
        override def populate() = {
          count += 1
          this() = i
        }
      }
      
      atom() mustEqual i
      atom() mustEqual i
      
      count mustEqual 1
    }
    
    "detect recursive self-population" in {
      val atom = new Atom[Int] {
        override def populate() = this() += 42
      }
      
      atom() must throwA[RuntimeException]
    }
    
    // TODO spec multi-thread behavior
  }
  
  "aggregate atoms" should {
    "store all values and return" in check { xs: Set[Int] =>
      val atom = new SetAtom[Int]
      xs foreach (atom +=)
      
      atom() mustEqual xs
      atom() mustEqual xs
    }
    
    "concatenate values and return" in check { (i: Int, xs: Set[Int]) =>
      val atom = new SetAtom[Int]
      atom += i
      atom ++= xs
      
      atom() mustEqual (xs + i)
      atom() mustEqual (xs + i)
    }
    
    "throw exception for mutation following force" in {
      val atom = new SetAtom[Int]
      atom += 42
      atom() mustEqual Set(42)
      
      (atom += 12) must throwA[RuntimeException]
      atom() mustEqual Set(42)
    }
  }
}
