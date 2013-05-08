/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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
