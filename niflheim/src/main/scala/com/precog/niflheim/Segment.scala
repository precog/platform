package com.precog.niflheim

import com.precog.common._
import com.precog.common.json._
import com.precog.util._

import blueeyes.json._

sealed abstract class Segment(val blockid: Long, val cpath: CPath, val ctype: CType, val defined: BitSet)

case class ArraySegment[A](id: Long, cp: CPath, ct: CValueType[A], d: BitSet, values: Array[A])
    extends Segment(id, cp, ct, d)

case class BooleanSegment(id: Long, cp: CPath, d: BitSet, values: BitSet)
    extends Segment(id, cp, CBoolean, d)

case class NullSegment[A](id: Long, cp: CPath, ct: CNullType, d: BitSet, values: Array[A])
    extends Segment(id, cp, ct, d)
