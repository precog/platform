/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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
