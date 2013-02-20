package com.precog.niflheim

import com.precog.common.CType
import com.precog.common.json.CPath
import com.precog.util._

trait StorageReader {
  def snapshot(pathConstraints: Option[Set[CPath]]): Seq[Segment]
  def structure: Iterable[(CPath, CType)]

  def id: Long

  /**
   * Returns the total length of the block.
   */
  def length: Int
}
