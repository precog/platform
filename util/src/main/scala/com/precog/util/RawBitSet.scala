package com.precog.util

import scala.annotation.tailrec


object RawBitSet {
  final def create(size: Int): RawBitSet = new Array[Int]((size >>> 5) + 1)

  final def get(bits: Array[Int], i: Int): Boolean = {
    val pos = i >>> 5
    if (pos < bits.length) {
      (bits(pos) & (1 << (i & 0x1F))) != 0
    } else {
      false
    }
  }

  final def set(bits: Array[Int], i: Int) {
    val pos = i >>> 5
    if (pos < bits.length) {
      bits(pos) |= (1 << (i & 0x1F))
    } else {
      throw new IndexOutOfBoundsException("Bit %d is out of range." format i)
    }
  }

  final def clear(bits: Array[Int], i: Int) {
    val pos = i >>> 5
    if (pos < bits.length) {
      bits(pos) &= ~(1 << (i & 0x1F))
    }
  }

  final def toArray(bits: Array[Int]): Array[Int] = {
    var n = 0
    var i = 0
    val len = bits.length
    while (i < len) {
      n += java.lang.Integer.bitCount(bits(i))
      i += 1
    }
    
    val ints = new Array[Int](n)
    
    @inline @tailrec
    def loopInts(bitsIndex: Int, intsIndex: Int) {
      if (bitsIndex < len) {
        loopInts(bitsIndex + 1, loopBits(bits(bitsIndex), 0, 0, intsIndex))
      }
    }
    
    @inline @tailrec
    def loopBits(bits: Int, shift: Int, value: Int, intsIndex: Int): Int = {
      if (((bits >> shift) & 1) == 1) {
        ints(intsIndex) = value
        if (shift < 31) loopBits(bits, shift + 1, value + 1, intsIndex + 1)
        else intsIndex
      } else {
        if (shift < 31) loopBits(bits, shift + 1, value + 1, intsIndex)
        else intsIndex
      }
    }
    
    loopInts(0, 0)
    ints
  }

  final def toList(bits: Array[Int]): List[Int] = {

    @inline @tailrec
    def rec0(n: Int, hi: Int, lo: Int, bs: List[Int]): List[Int] = {
      if (lo >= 0) {
        if ((n & (1 << lo)) != 0) {
          rec0(n, hi, lo - 1, (hi | lo) :: bs)
        } else {
          rec0(n, hi, lo - 1, bs)
        }
      } else {
        bs
      }
    }

    @inline @tailrec
    def rec(i: Int, bs: List[Int]): List[Int] = {
      if (i >= 0) {
        rec(i - 1, rec0(bits(i), i << 5, 31, bs))
      } else {
        bs
      }
    }

    rec(bits.length - 1, Nil)
  }
}

