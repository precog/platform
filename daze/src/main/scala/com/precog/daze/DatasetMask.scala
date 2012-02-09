package com.precog
package daze

import akka.dispatch.Future
import scalaz.effect._

import com.precog.yggdrasil._

trait DatasetMask[X] {
  def derefObject(field: String): DatasetMask[X] 
  def derefArray(index: Int): DatasetMask[X]
  def typed(tpe: SType): DatasetMask[X]
  def realize: DatasetEnum[X, SEvent, IO]
}
