package com.precog.ingest

import com.precog.common.ingest._
import com.precog.util.PrecogUnit

import akka.dispatch.Future
import akka.util.Timeout

import scalaz._

case class StoreFailure(message: String)

trait EventStore[M[+_]] {
  def save(action: Event, timeout: Timeout): M[StoreFailure \/ PrecogUnit]
}
