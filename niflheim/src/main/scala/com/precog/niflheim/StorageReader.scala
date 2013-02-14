package com.precog.niflheim

import com.precog.common.CType
import com.precog.common.json.CPath

trait StorageReader {
  def snapshot: Segments
  def structure: Iterable[(CPath, CType)]

  def id: Long
  def length: Int
}
