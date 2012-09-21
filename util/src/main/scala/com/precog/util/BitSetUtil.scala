package com.precog.util

import scala.annotation.tailrec

object BitSetUtil {
  class BitSetOperations(bs: BitSet) {
    def toArray() = bitSetToArray(bs)
    def toList() = bitSetToList(bs)
    def apply(n: Int): Boolean = bs.get(n)
    def update(n: Int, b: Boolean): Unit = bs.set(n, b)

    def isEmpty(): Boolean = bs.size == 0
    def min(): Int = 999
    def max(): Int = 999
    def foreach(f: Int => Unit) = {}
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
    val len = bs.getBitsLength
    var j = 0
    val arr = new Array[Long](len)

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

    loopLongs(0, bs.getBits, len - 1, 0)
    arr
  }

  def bitSetToList(bs: BitSet): List[Int] = {
    @tailrec
    def loopBits(long: Long, bit: Int, base: Int, sofar: List[Int]): List[Int] = {
      if (bit < 0)
        sofar
      else if (((long >> bit) & 1) == 1)
        loopBits(long, bit - 1, base, (base + bit) :: sofar)
      else
        loopBits(long, bit - 1, base, sofar)
    }

    @tailrec
    def loopLongs(i: Int, longs: Array[Long], base: Int, sofar: List[Int]): List[Int] = {
      if (i < 0)
        sofar
      else
        loopLongs(i - 1, longs, base - 64, loopBits(longs(i), 63, base, sofar))
    }

    val last = bs.getBitsLength - 1
    loopLongs(last, bs.getBits, last * 64, Nil)
  }
}
