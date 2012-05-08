package com.precog.yggdrasil
package iterable

import com.precog.common.Path

trait StorageEngineQueryAPI[Dataset[_]] {
  def fullProjection(userUID: String, path: Path, expiresAt: Long, release: Release): Dataset[SValue]
  def mask(userUID: String, path: Path): DatasetMask[Dataset[SValue]]
}
