package com.precog
package daze

import akka.dispatch.ExecutionContext
import scalaz.effect._

import com.precog.yggdrasil._

trait DatasetMask[Dataset[_]] {
  def derefObject(field: String): DatasetMask[Dataset]
  def derefArray(index: Int): DatasetMask[Dataset]
  def typed(tpe: SType): DatasetMask[Dataset]
  def realize(expiresAt: Long): Dataset[SValue]
}
