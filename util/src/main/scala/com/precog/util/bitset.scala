package com.precog.util

import scala.collection._

package object bitset {
  def makeMutable(bitSet: BitSet): mutable.BitSet = (mutable.BitSet.canBuildFrom() ++= bitSet.iterator).result()
}

// vim: set ts=4 sw=4 et:
