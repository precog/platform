package com.precog.niflheim

import com.precog.common._
import com.precog.util._

trait StorageReader {
  def snapshot(pathConstraints: Option[Set[CPath]]): Seq[Segment]
  def structure: Iterable[(CPath, CType)]

  def isStable: Boolean

  def id: Long

  /**
   * Returns the total length of the block.
   */
  def length: Int
}
