package com.precog
package daze

import akka.dispatch.ExecutionContext
import scalaz.effect._

import com.precog.yggdrasil._

trait DatasetMask[X] {
  def derefObject(field: String): DatasetMask[X] 
  def derefArray(index: Int): DatasetMask[X]
  def typed(tpe: SType): DatasetMask[X]
  def realize(implicit asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO]
}
