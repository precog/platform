package com.precog.niflheim

import com.precog.common._
import com.precog.util._

trait StorageReader {
  def snapshot(pathConstraints: Option[Set[CPath]]): Block
  def snapshotRef(refConstraints: Option[Set[ColumnRef]]): Block
  def structure: Iterable[ColumnRef]

  def isStable: Boolean

  def id: Long

  /**
   * Returns the total length of the block.
   */
  def length: Int

  override def toString = "StorageReader: id = %d, length = %d, structure = %s".format(id, length, structure)
}
