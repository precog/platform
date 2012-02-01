package com.precog.ingest.service
package external

import blueeyes.core.data.{ByteChunk, Bijection, BijectionsChunkJson}
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json.JsonAST._
//import rosetta.json.blueeyes._

import akka.dispatch.Future

import java.net.InetAddress
import java.util.Date
import scalaz.Scalaz._

//import com.precog.instrumentation.blueeyes.ReportGridInstrumentation
//import com.precog.api.ReportGridTrackingClient
//import com.precog.api.blueeyes._
//import com.precog.api.Tag

//import com.precog.analytics._

//object NoopTrackingClient extends ReportGridTrackingClient[JValue](JsonBlueEyes) {
//  override def track(path: com.precog.api.Path, name: String, properties: JValue = JsonBlueEyes.EmptyObject, rollup: Boolean = false, tags: Set[Tag[JValue]] = Set.empty[Tag[JValue]], count: Option[Int] = None, headers: Map[String, String] = Map.empty): Unit = {
//    //println("Tracked " + path + "; " + name + " - " + properties)
//  }
//}

// vim: set ts=4 sw=4 et:
