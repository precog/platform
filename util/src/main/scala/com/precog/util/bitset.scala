package com.precog.util

import scala.collection.mutable
import scala.collection.{BitSet => ScalaBitSet}

package object bitset {
  def makeMutable(bitSet: ScalaBitSet): mutable.BitSet = (mutable.BitSet.canBuildFrom() ++= bitSet.iterator).result()
}

// vim: set ts=4 sw=4 et:
