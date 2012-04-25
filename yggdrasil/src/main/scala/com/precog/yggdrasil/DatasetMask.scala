package com.precog.yggdrasil

trait DatasetMask[Dataset] {
  def derefObject(field: String): DatasetMask[Dataset]
  def derefArray(index: Int): DatasetMask[Dataset]
  def typed(tpe: SType): DatasetMask[Dataset]
  def realize(expiresAt: Long): Dataset
}
