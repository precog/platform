package com.precog.ingest

import com.precog.common.ingest._
import com.precog.util.PrecogUnit

import akka.dispatch.Future
import akka.util.Timeout

trait EventStore {
  // Returns a Future that completes when the storage layer has taken ownership of and
  // acknowledged the receipt of the data.
  def save(action: Event, timeout: Timeout): Future[PrecogUnit]
  def start: Future[PrecogUnit]
  def stop: Future[PrecogUnit]
}
