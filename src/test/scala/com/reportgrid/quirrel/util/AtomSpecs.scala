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
