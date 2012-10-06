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

import scala.annotation.tailrec
import scala.collection.mutable

/**
 * Unchecked and unboxed (fast!) deque implementation with a fixed bound.  None
 * of the operations on this datastructure are checked for bounds.  You are
 * trusted to get this right on your own.  If you do something weird, you could
 * end up overwriting data, reading old results, etc.  Don't do that.
 *
 * No objects were allocated in the making of this film.
 */
final class RingDeque[@specialized(Boolean, Int, Long, Double, Float, Short) A: ClassManifest](_bound: Int) {
  val bound = _bound + 1
  
  private val ring = new Array[A](bound)
  private var front = 0
  private var back = rotate(front, 1)
  
  def isEmpty = front == rotate(back, -1)
  
  def empty() {
    back = rotate(front, 1)
  }
  
  def popFront(): A = {
    val result = ring(front)
    moveFront(1)
    result
  }
  
  def pushFront(a: A) {
    moveFront(-1)
    ring(front) = a
  }
  
  def removeFront(length: Int) {
    moveFront(length)
  }
  
  def popBack(): A = {
    moveBack(-1)
    ring(rotate(back, -1))
  }
  
  def pushBack(a: A) {
    ring(rotate(back, -1)) = a
    moveBack(1)
  }
  
  def removeBack(length: Int) {
    moveBack(-length)
  }
  
  def length: Int =
    (if (back > front) back - front else (back + bound) - front) - 1 
  
  /**
   * SLOW!!  Don't use in production code.
   */
  def toList: List[A] = {
    val end = front + length
    val results = for (i <- front until end)
      yield ring(i % bound)
    
    results.toList
  }
  
  @inline
  private[this] def rotate(target: Int, delta: Int) =
    (target + delta + bound) % bound
  
  @inline
  private[this] def moveFront(delta: Int) {
    front = rotate(front, delta)
  }
  
  @inline
  private[this] def moveBack(delta: Int) {
    back = rotate(back, delta)
  }
}
