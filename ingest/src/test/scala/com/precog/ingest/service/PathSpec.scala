package com.precog.ingest
package service 

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck
import org.scalacheck._
import org.scalacheck.Gen._
import org.scalacheck.Prop._

import com.precog.analytics.Path

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
  }
}
