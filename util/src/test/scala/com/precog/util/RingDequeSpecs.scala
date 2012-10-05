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
package com.precog.util

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck._

object RingDequeSpecs extends Specification with ScalaCheck {
  implicit val params = set(
    minTestsOk -> 2500,
    workers -> Runtime.getRuntime.availableProcessors)
  
  "unsafe ring deque" should {
    "implement prepend" in check { (xs: List[Int], x: Int) =>
      val result = fromList(xs, xs.length + 1)
      result.pushFront(x)
      
      result.toList mustEqual (x +: xs)
    }
    
    "implement append" in check { (xs: List[Int], x: Int) =>
      val result = fromList(xs, xs.length + 1)
      result.pushBack(x)
      
      result.toList mustEqual (xs :+ x)
    }
    
    "implement popFront" in check { (xs: List[Int], x: Int) =>
      val result = fromList(xs, xs.length + 1)
      result.pushFront(x)
      
      result.popFront() mustEqual x
      result.toList mustEqual xs
    }
    
    "implement popBack" in check { (xs: List[Int], x: Int) =>
      val result = fromList(xs, xs.length + 1)
      result.pushBack(x)
      
      result.popBack() mustEqual x
      result.toList mustEqual xs
    }
    
    "implement length" in check { xs: List[Int] =>
      fromList(xs, xs.length + 10).length mustEqual xs.length
      fromList(xs, xs.length).length mustEqual xs.length
    }
    
    "satisfy identity" in check { xs: List[Int] =>
      fromList(xs, xs.length).toList mustEqual xs
    }
    
    "append a full list following a half-appending" in check { xs: List[Int] =>
      val deque = new RingDeque[Int](xs.length)
      xs take (xs.length / 2) foreach deque.pushBack
      (0 until (xs.length / 2)) foreach { _ => deque.popFront() }
      xs foreach deque.pushBack
      deque.toList mustEqual xs
    }
    
    "reverse a list by prepending" in check { xs: List[Int] =>
      val deque = new RingDeque[Int](xs.length)
      xs foreach deque.pushFront
      deque.toList mustEqual xs.reverse
    }
  }
  
  private def fromList(xs: List[Int], bound: Int): RingDeque[Int] =
    xs.foldLeft(new RingDeque[Int](bound)) { (deque, x) => deque pushBack x; deque }
} 
