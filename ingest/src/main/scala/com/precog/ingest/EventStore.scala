package com.precog.ingest

import com.precog.common.ingest._
import com.precog.util.PrecogUnit

import akka.dispatch.Future
import akka.util.Timeout

trait EventStore[M[+_]] {
  def save(action: Event, timeout: Timeout): M[PrecogUnit]
}
