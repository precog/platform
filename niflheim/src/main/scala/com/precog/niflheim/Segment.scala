package com.precog.niflheim

import com.precog.common._
import com.precog.common.json._
import com.precog.util._
import com.precog.util.BitSetUtil.Implicits._

import blueeyes.json._

sealed abstract class Segment(val blockid: Long, val cpath: CPath, val ctype: CType, val defined: BitSet) {
  def length: Int
}

case class ArraySegment[A: ClassManifest](id: Long, cp: CPath, ct: CValueType[A], d: BitSet, values: Array[A]) extends Segment(id, cp, ct, d) {
  def ++(rhs: ArraySegment[A]): ArraySegment[A] = rhs match {
    case ArraySegment(`id`, `cp`, `ct`, d2, values2) =>
      ArraySegment(id, cp, ct, d ++ d2, (values ++ values2).toArray)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }
  def length = values.length
}

case class BooleanSegment(id: Long, cp: CPath, d: BitSet, values: BitSet, val length: Int) extends Segment(id, cp, CBoolean, d) {
  def ++(rhs: BooleanSegment): BooleanSegment = rhs match {
    case BooleanSegment(`id`, `cp`, d2, values2, length2) =>
      BooleanSegment(id, cp, d ++ d2, values ++ values2, length + length2)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }
}

case class NullSegment[A](id: Long, cp: CPath, ct: CNullType, d: BitSet, val length: Int) extends Segment(id, cp, ct, d) {
  def ++(rhs: NullSegment[A]): NullSegment[A] = rhs match {
    case NullSegment(`id`, `cp`, `ct`, d2, length2) =>
      NullSegment(id, cp, ct, d ++ d2, length + length2)
    case _ =>
      throw new IllegalArgumentException("mismatched Segments: %s and %s" format (this, rhs))
  }
}
