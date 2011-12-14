package com.querio.ingest.service
package external

import blueeyes.concurrent.Future
import blueeyes.core.data.{ByteChunk, Bijection, BijectionsChunkJson}
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json.JsonAST._
import rosetta.json.blueeyes._

import java.net.InetAddress
import java.util.Date
import scalaz.Scalaz._

import com.querio.instrumentation.blueeyes.ReportGridInstrumentation
import com.reportgrid.api.ReportGridTrackingClient
import com.reportgrid.api.blueeyes._
import com.reportgrid.api.Tag

import com.reportgrid.analytics._

object NoopTrackingClient extends ReportGridTrackingClient[JValue](JsonBlueEyes) {
  override def track(path: com.reportgrid.api.Path, name: String, properties: JValue = JsonBlueEyes.EmptyObject, rollup: Boolean = false, tags: Set[Tag[JValue]] = Set.empty[Tag[JValue]], count: Option[Int] = None, headers: Map[String, String] = Map.empty): Unit = {
    //println("Tracked " + path + "; " + name + " - " + properties)
  }
}

// vim: set ts=4 sw=4 et:
