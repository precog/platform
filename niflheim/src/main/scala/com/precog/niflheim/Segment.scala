package com.precog.niflheim

import com.precog.common._
import com.precog.common.json._
import com.precog.util._
import com.precog.util.BitSetUtil.Implicits._

import blueeyes.json._

case class SegmentId(blockid: Long, cpath: CPath, ctype: CType)

sealed trait Segment {
  def id: SegmentId = SegmentId(blockid, cpath, ctype)

  def blockid: Long
  def cpath: CPath
  def ctype: CType
  def defined: BitSet
  def length: Int
  def extend(amount: Int): Segment
}

case class ArraySegment[A](blockid: Long, cpath: CPath, ctype: CValueType[A], defined: BitSet, values: Array[A]) extends Segment {
  private implicit val m = ctype.manifest

  override def equals(that: Any): Boolean = that match {
    case ArraySegment(`blockid`, `cpath`, ct2, d2, values2) =>
      if (ctype != ct2 || defined != d2 || values2.length != values.length) return false
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
    case ArraySegment(`blockid`, `cpath`, `ctype`, d2, values2) =>
      ArraySegment(blockid, cpath, ctype, defined ++ d2, (values ++ values2).toArray)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }

  def length = values.length

  def extend(amount: Int) = {
    val arr = new Array[A](values.length + amount)
    var i = 0
    val len = values.length
    while (i < len) { arr(i) = values(i); i += 1 }
    ArraySegment(blockid, cpath, ctype, defined.copy, arr)
  }
}

case class BooleanSegment(blockid: Long, cpath: CPath, defined: BitSet, values: BitSet, length: Int) extends Segment {
  val ctype = CBoolean

  override def equals(that: Any) = that match {
    case BooleanSegment(`blockid`, `cpath`, d2, values2, `length`) => defined == d2 && values == values2
    case _ => false
  }

  def ++(rhs: BooleanSegment): BooleanSegment = rhs match {
    case BooleanSegment(`blockid`, `cpath`, d2, values2, length2) =>
      BooleanSegment(blockid, cpath, defined ++ d2, values ++ values2, length + length2)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }

  def extend(amount: Int) = BooleanSegment(blockid, cpath, defined.copy, values.copy, length + amount)
}

case class NullSegment(blockid: Long, cpath: CPath, ctype: CNullType, defined: BitSet, length: Int) extends Segment {

  override def equals(that: Any) = that match {
    case NullSegment(`blockid`, `cpath`, `ctype`, d2, `length`) => defined == d2
    case _ => false
  }

  def ++(rhs: NullSegment): NullSegment = rhs match {
    case NullSegment(`blockid`, `cpath`, `ctype`, d2, length2) =>
      NullSegment(blockid, cpath, ctype, defined ++ d2, length + length2)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }

  def extend(amount: Int) = NullSegment(blockid, cpath, ctype, defined.copy, length + amount)
}
