package com.precog.yggdrasil

import com.precog.common.Path

trait StorageEngineQueryAPI[Dataset] {
  def fullProjection(userUID: String, path: Path, expiresAt: Long): Dataset
  def mask(userUID: String, path: Path): DatasetMask[Dataset]
}

// vim: set ts=4 sw=4 et:
