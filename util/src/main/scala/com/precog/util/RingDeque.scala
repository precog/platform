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
  private[this] def rotate(target: Int, delta: Int) = {
    val back = (target + delta) % bound
    if (back < 0)
      back + bound
    else
      back
  }
  
  @inline
  private[this] def moveFront(delta: Int) {
    front = rotate(front, delta)
  }
  
  @inline
  private[this] def moveBack(delta: Int) {
    back = rotate(back, delta)
  }
}
