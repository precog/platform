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
package com.precog
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
    
    "receive another atom's valuation when linked prior to assignment" in {
      val a1 = atom[Int]
      val a2 = atom[Int]
      a1.from(a2)
      
      a2() = 42
      a2() mustEqual 42
      
      a1() mustEqual 42
    }
    
    "receive another atom's valuation when linked after to assignment" in {
      val a1 = atom[Int]
      val a2 = atom[Int]
      
      a2() = 42
      a2() mustEqual 42
      
      a1.from(a2)
      
      a1() mustEqual 42
    }
    
    "disregard link when overridden by value" in ({
      {
        val a1 = atom[Int]
        val a2 = atom[Int]
        a1.from(a2)
        a1() = 24
        
        a2() = 42
        
        // ordering might matter here
        a2() mustEqual 42
        a1() mustEqual 24
      }
      
      {
        val a1 = atom[Int]
        val a2 = atom[Int]
        a1.from(a2)
        a1() = 24
        
        a2() = 42
        
        // ordering might matter here
        a1() mustEqual 24
        a2() mustEqual 42
      }
    } pendingUntilFixed)
    
    // TODO spec multi-thread behavior
  }
  
  "aggregate atoms" should {
    "self-populate once on access using =" in check { xs: Set[Int] =>
      lazy val a: Atom[Set[Int]] = atom[Set[Int]] {
        a() = xs
      }
      
      a() mustEqual xs
    }
    
    "self-populate once on access using +=" in check { xs: Set[Int] =>
      lazy val a: Atom[Set[Int]] = atom[Set[Int]] {
        a ++= Set[Int]()
        xs foreach { x => a += x }
      }
      
      a() mustEqual xs
    }
    
    "self-populate once on access using ++=" in check { xs: Set[Int] =>
      lazy val a: Atom[Set[Int]] = atom[Set[Int]] {
        a ++= xs
      }
      
      a() mustEqual xs
    }
    
    "never self-populate on access when pre-populated by =" in check { xs: Set[Int] =>
      val marker = Stream from 0 dropWhile xs head
      
      lazy val a: Atom[Set[Int]] = atom[Set[Int]] {
        a += marker
      }
      
      a() = xs
      a() mustEqual xs
    }
    
    "self-populate once on access when pre-populated by +=" in check { xs: Set[Int] =>
      val marker = Stream from 0 dropWhile xs head
      
      lazy val a: Atom[Set[Int]] = atom[Set[Int]] {
        a += marker
      }
      
      xs foreach { x => a += x }
      a() mustEqual (xs + marker)
    }
    
    "self-populate once on access when pre-populated by ++=" in check { xs: Set[Int] =>
      val marker = Stream from 0 dropWhile xs head
      
      lazy val a: Atom[Set[Int]] = atom[Set[Int]] {
        a += marker
      }
      
      a ++= xs
      a() mustEqual (xs + marker)
    }
    
    "store all values and return" in check { xs: Set[Int] =>
      lazy val a: Atom[Set[Int]] = atom[Set[Int]] {
        a ++= Set[Int]()
      }
      
      xs foreach { x => a += x }
      
      a() mustEqual xs
      a() mustEqual xs
    }
    
    "concatenate values and return" in check { (i: Int, xs: Set[Int]) =>
      lazy val a: Atom[Set[Int]] = atom[Set[Int]] {
        a ++= Set[Int]()
      }
      
      a += i
      a ++= xs
      
      a() mustEqual (xs + i)
      a() mustEqual (xs + i)
    }
    
    "silently fail for mutation following force" in {
      lazy val a: Atom[Set[Int]] = atom[Set[Int]] {
        a ++= Set[Int]()
      }
      
      a += 42
      a() mustEqual Set(42)
      
      (a += 12) must not(throwA[RuntimeException])
      a() mustEqual Set(42)
    }
  }
}
