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
      ctype == ct2 && defined == d2 && arrayEq[A](values, values2.asInstanceOf[Array[A]])
    case _ =>
      false
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
    case BooleanSegment(`blockid`, `cpath`, d2, values2, `length`) =>
      defined == d2 && values == values2
    case _ =>
      false
  }

  def extend(amount: Int) = BooleanSegment(blockid, cpath, defined.copy, values.copy, length + amount)
}

case class NullSegment(blockid: Long, cpath: CPath, ctype: CNullType, defined: BitSet, length: Int) extends Segment {

  override def equals(that: Any) = that match {
    case NullSegment(`blockid`, `cpath`, `ctype`, d2, `length`) =>
      defined == d2
    case _ =>
      false
  }

  def extend(amount: Int) = NullSegment(blockid, cpath, ctype, defined.copy, length + amount)
}
