package com.precog.niflheim

import com.precog.common._
import com.precog.common.json._
import com.precog.util._
import com.precog.util.BitSetUtil.Implicits._

import scala.{ specialized => spec }

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

sealed trait ValueSegment[@spec(Boolean,Long,Double) A] extends Segment {
  def ctype: CValueType[A]
  def map[@spec(Boolean,Long,Double) B: CValueType: Manifest](f: A => B): ValueSegment[B]

  def normalize: ValueSegment[A] = this match {
    case seg: ArraySegment[_] if seg.ctype == CBoolean =>
      val values0 = seg.values.asInstanceOf[Array[Boolean]]
      val values = BitSetUtil.create()
      defined.foreach { row =>
        values(row) = values0(row)
      }
      BooleanSegment(blockid, cpath, defined, values, values.length).asInstanceOf[ValueSegment[A]]

    case _ =>
      this
  }
}

case class ArraySegment[@spec(Boolean,Long,Double) A](blockid: Long, cpath: CPath, ctype: CValueType[A], defined: BitSet, values: Array[A]) extends ValueSegment[A] {
  private implicit def m = ctype.manifest

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

  def map[@spec(Boolean,Long,Double) B: CValueType: Manifest](f: A => B): ValueSegment[B] = {
    val values0 = new Array[B](values.length)
    defined.foreach { row =>
      values0(row) = f(values(row))
    }
    ArraySegment[B](blockid, cpath, CValueType[B], defined, values0).normalize
  }
}

case class BooleanSegment(blockid: Long, cpath: CPath, defined: BitSet, values: BitSet, length: Int) extends ValueSegment[Boolean] {
  val ctype = CBoolean

  override def equals(that: Any) = that match {
    case BooleanSegment(`blockid`, `cpath`, d2, values2, `length`) =>
      defined == d2 && values == values2
    case _ =>
      false
  }

  def extend(amount: Int) = BooleanSegment(blockid, cpath, defined.copy, values.copy, length + amount)

  def map[@spec(Boolean,Long,Double) B: CValueType: Manifest](f: Boolean => B): ValueSegment[B] = {
    val values0 = new Array[B](values.length)
    defined.foreach { row =>
      values0(row) = f(values(row))
    }
    ArraySegment[B](blockid, cpath, CValueType[B], defined, values0).normalize
  }
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
