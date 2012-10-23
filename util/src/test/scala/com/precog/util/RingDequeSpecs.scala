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
