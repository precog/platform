package com.precog.daze

import com.precog.yggdrasil.SValue
import com.precog.common.Path

trait StorageEngineQueryAPI[Dataset[_]] {
  def fullProjection(userUID: String, path: Path, expiresAt: Long): Dataset[SValue]
  def mask(userUID: String, path: Path): DatasetMask[Dataset]
}

// vim: set ts=4 sw=4 et:
