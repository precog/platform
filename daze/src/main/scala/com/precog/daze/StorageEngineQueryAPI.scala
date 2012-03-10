package com.precog.daze

import com.precog.yggdrasil.SEvent
import com.precog.common.Path

import akka.dispatch.ExecutionContext

import scalaz.effect.IO

trait StorageEngineQueryAPI {
  type X = Throwable

  def fullProjection(userUID: String, path: Path, expiresAt: Long)(implicit asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO]
  def mask(userUID: String, path: Path): DatasetMask[X]
}

// vim: set ts=4 sw=4 et:
