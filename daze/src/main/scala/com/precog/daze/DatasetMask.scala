package com.precog
package daze

import scalaz.effect._

import com.precog.yggdrasil._

// stubbed for now
trait DatasetMask {
  def derefObject(field: String): DatasetMask
  def derefArray(index: Int): DatasetMask
  def typed(tpe: SType): DatasetMask
  def realize[X]: DatasetEnum[X, SEvent, IO]
}
