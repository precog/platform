package com.precog.niflheim

import com.precog.common._

trait StorageReader {
  def snapshot(pathConstraints: Option[Set[CPath]]): Seq[Segment]
  def structure: Iterable[(CPath, CType)]

  def id: Long
  def length: Int
}
