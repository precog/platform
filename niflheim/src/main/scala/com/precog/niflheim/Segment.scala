package com.precog.niflheim

import com.precog.common._
import com.precog.common.json._
import com.precog.util._
import com.precog.util.BitSetUtil.Implicits._

import blueeyes.json._

sealed abstract class Segment(val blockid: Long, val cpath: CPath, val ctype: CType, val defined: BitSet) {
  def length: Int
  def extend(amount: Int): Segment
}

case class ArraySegment[A: ClassManifest](id: Long, cp: CPath, ct: CValueType[A], d: BitSet, val values: Array[A]) extends Segment(id, cp, ct, d) {

  override def equals(that: Any): Boolean = that match {
    case ArraySegment(`id`, `cp`, ct2, d2, values2) =>
      if (ct != ct2 || d != d2 || values2.length != values.length) return false
      var i = 0
      val len = values.length
      while (i < len) {
        if (values2(i) != values(i)) return false
        i += 1
      }
      true
    case _ =>
      false
  }

  def ++(rhs: ArraySegment[A]): ArraySegment[A] = rhs match {
    case ArraySegment(`id`, `cp`, `ct`, d2, values2) =>
      ArraySegment(id, cp, ct, d ++ d2, (values ++ values2).toArray)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }

  def length = values.length

  def extend(amount: Int) = {
    val arr = new Array[A](values.length + amount)
    var i = 0
    val len = values.length
    while (i < len) { arr(i) = values(i); i += 1 }
    ArraySegment(id, cp, ct, d.copy, arr)
  }
}

case class BooleanSegment(id: Long, cp: CPath, d: BitSet, val values: BitSet, val length: Int) extends Segment(id, cp, CBoolean, d) {

  override def equals(that: Any) = that match {
    case BooleanSegment(`id`, `cp`, d2, values2, `length`) => d == d2 && values == values2
    case _ => false
  }

  def ++(rhs: BooleanSegment): BooleanSegment = rhs match {
    case BooleanSegment(`id`, `cp`, d2, values2, length2) =>
      BooleanSegment(id, cp, d ++ d2, values ++ values2, length + length2)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }

  def extend(amount: Int) = BooleanSegment(id, cp, d.copy, values.copy, length + amount)
}

case class NullSegment(id: Long, cp: CPath, ct: CNullType, d: BitSet, val length: Int) extends Segment(id, cp, ct, d) {

  override def equals(that: Any) = that match {
    case NullSegment(`id`, `cp`, `ct`, d2, `length`) => d == d2
    case _ => false
  }

  def ++(rhs: NullSegment): NullSegment = rhs match {
    case NullSegment(`id`, `cp`, `ct`, d2, length2) =>
      NullSegment(id, cp, ct, d ++ d2, length + length2)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }

  def extend(amount: Int) = NullSegment(id, cp, ct, d.copy, length + amount)
}
