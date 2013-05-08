package com.precog.common

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Prop._

class PathSpec extends Specification with ScalaCheck {
  "rollups for a path" should {
    "not roll up when flag is false" in {
      val sample = Path("/my/fancy/path")
      sample.rollups(0) must_== List(sample)
    }

    "include the original path" in {
      val sample = Path("/my/fancy/path")
      sample.rollups(3) must haveTheSameElementsAs(
        sample :: 
        Path("/my/fancy") :: 
        Path("/my") :: 
        Path("/") :: Nil
      )
    }
    
    "Roll up a limited distance" in {
      val sample = Path("/my/fancy/path")
      sample.rollups(2) must haveTheSameElementsAs(
        sample :: 
        Path("/my/fancy") :: 
        Path("/my") :: Nil
      )
    }

    "drop a matching prefix using '-'" in {
      todo
    } 
    
    "Correctly identify child paths" in {
      val parent = Path("/my/fancy/path")
      val identical = Path("/my/fancy/path")
      val child1 = Path("/my/fancy/path/child")
      val child2 = Path("/my/fancy/path/grand/child")
      val notChild1 = Path("/other/fancy/path")
      val notChild2 = Path("/my/fancy/")

      parent.isEqualOrParentOf(parent) must beTrue
      parent.isEqualOrParentOf(identical) must beTrue
      parent.isEqualOrParentOf(child1) must beTrue
      parent.isEqualOrParentOf(child2) must beTrue

      parent.isEqualOrParentOf(notChild1) must beFalse
      parent.isEqualOrParentOf(notChild2) must beFalse
      
    }
  }
}
