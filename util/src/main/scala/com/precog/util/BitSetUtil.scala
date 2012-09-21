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

object BitSetUtil {
  class BitSetOperations(bs: BitSet) {
    def toUnboxedArray(): Array[Long] = bitSetToArray(bs)
    def toList(): List[Long] = bitSetToList(bs)

    def min(): Int = {
      val n = bs.nextSetBit(0)
      if (n < 0) sys.error("can't take min of empty set") else n
    }

    def max(): Int = {
      @tailrec
      def findBit(i: Int, last: Int): Int = {
        val j = bs.nextSetBit(i)
        if (j < 0) last else findBit(j + 1, j)
      }

      val ns = bs.getBits
      var i = ns.length - 1
      while (i >= 0) {
        if (ns(i) != 0) return findBit(i * 64, -1)
        i -= 1
      }
      sys.error("can't find max of empty set")
    }

    def foreach(f: Int => Unit) = {
      var b = bs.nextSetBit(0)
      while (b >= 0) {
        f(b)
        b = bs.nextSetBit(b + 1)
      }
    }
  }

  object Implicits {
    implicit def bitSetOps(bs: BitSet) = new BitSetOperations(bs)
  }

  def fromArray(arr: Array[Long]) = {
    val bs = new BitSet()
    bs.setBits(arr)
    bs
  }

  def create(): BitSet = new BitSet()

  def create(ns: Array[Int]): BitSet = {
    val bs = new BitSet()
    var i = 0
    val len = ns.length
    while (i < len) {
      bs.set(ns(i))
      i += 1
    }
    bs
  }

  def create(ns: Seq[Int]): BitSet = {
    val bs = new BitSet()
    ns.foreach(n => bs.set(n))
    bs
  }

  def bitSetToArray(bs: BitSet): Array[Long] = {
    var j = 0
    val arr = new Array[Long](bs.size)

    @tailrec
    def loopBits(long: Long, bit: Int, base: Int) {
      if (((long >> bit) & 1) == 1) {
        arr(j) = base + bit
        j += 1
      }
      if (bit < 63)
        loopBits(long, bit + 1, base)
    }

    @tailrec
    def loopLongs(i: Int, longs: Array[Long], last: Int, base: Int) {
      loopBits(longs(i), 0, base)
      if (i < last)
        loopLongs(i + 1, longs, last, base + 64)
    }

    loopLongs(0, bs.getBits, bs.getBitsLength - 1, 0)
    arr
  }

  def bitSetToList(bs: BitSet): List[Long] = {
    @tailrec
    def loopBits(long: Long, bit: Int, base: Int, sofar: List[Long]): List[Long] = {
      if (bit < 0)
        sofar
      else if (((long >> bit) & 1) == 1)
        loopBits(long, bit - 1, base, (base + bit) :: sofar)
      else
        loopBits(long, bit - 1, base, sofar)
    }

    @tailrec
    def loopLongs(i: Int, longs: Array[Long], base: Int, sofar: List[Long]): List[Long] = {
      if (i < 0)
        sofar
      else
        loopLongs(i - 1, longs, base - 64, loopBits(longs(i), 63, base, sofar))
    }

    val last = bs.getBitsLength - 1
    loopLongs(last, bs.getBits, last * 64, Nil)
  }
}
