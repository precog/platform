package com.precog.daze

import com.precog.analytics.Path
import com.precog.yggdrasil.SEvent

import akka.dispatch.ExecutionContext

import scalaz.effect.IO

trait StorageEngineQueryAPI {
  def fullProjection[X](path: Path)(implicit asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO]
  def mask[X](path: Path): DatasetMask[X]
}

// vim: set ts=4 sw=4 et:
