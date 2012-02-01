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
package com.precog.instrumentation
package blueeyes 

import _root_.blueeyes._
import _root_.blueeyes.util._
import _root_.blueeyes.core.http._
import _root_.blueeyes.core.http.HttpHeaders._
import _root_.blueeyes.core.service._
import _root_.blueeyes.json.JsonAST._
import _root_.blueeyes.json.xschema.DefaultSerialization._
import com.precog.analytics.Token
import com.precog.analytics.TokenManager
//import com.precog.api._
//import rosetta.json.blueeyes._
import org.joda.time.Duration
import org.joda.time.Instant

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import scalaz.Scalaz._

//trait ReportGridInstrumentation {
//
//  implicit def defaultFutureDispatch: MessageDispatcher
//
//  val ReportGridUserAgent = "ReportGrid Introspection Agent / 1.0"
//
//  def bucketCounts(i: Long) = if (i / 25000 > 0) (i / 10000) * 10000
//                              else if (i / 2500 > 0) (i / 1000) * 1000
//                              else if (i / 250 > 0) (i / 100) * 100
//                              else if (i / 25 > 0) (i / 10) * 10
//                              else i
//
//  trait Auditor {
//    implicit def service2Audited[A, B](service: HttpService[A, B]): Audited[A, B]
//    def audited[A, B](name: String) = service2Audited(_: HttpService[A, B]).audited(name)
//  }
//
//  trait Audited[A, B] {
//    def audited(name: String): HttpService[A, B]
//  }
//
//  def auditor(client: ReportGridTrackingClient[JValue], clock: Clock, tokenManager: TokenManager) = {
//    new Auditor {
//      implicit def service2Audited[A, B](service: HttpService[A, B]): Audited[A, B] = new Audited[A, B] {
//        def audited(name: String): HttpService[A, B] = {
//          val auditService = record(client, clock, service) {
//            (req: HttpRequest[A], resp: B, start: Instant, end: Instant) => {
//              req.parameters.get('tokenId).map(tokenManager.lookup).getOrElse(Future(None)) map { 
//                token => Trackable(
//                  path = "/" + token.map(_.accountTokenId).getOrElse("anonymous") + "/latencies",
//                  name = "request",
//                  properties = JObject(List(
//                    JField("latency", bucketCounts(new Duration(start, end).getMillis)),
//                    JField("method", req.method.toString),
//                    JField("function", name) //TODO: Make this more informative
//                  )),
//                  rollup = true,
//                  tags = Set(TimeTag[JValue](start.toDate))
//                )
//              }
//            }
//          }
//        
//          auditService withMetadata DescriptionMetadata(name)
//        }
//      }
//    }
//  }
//
//  def record[A, B](client: ReportGridTrackingClient[JValue], clock: Clock, next: HttpService[A, B])
//                  (f: (HttpRequest[A], B, Instant, Instant) => Future[Trackable[JValue]]) = new DelegatingService[A, B, A, B] {
//    override val delegate = next
//    override val service = (req: HttpRequest[A]) => {
//      if (req.headers.header[`User-Agent`].exists(_.value == ReportGridUserAgent)) delegate.service(req)
//      else {
//        val start = clock.instant
//        delegate.service(req) map { resp =>
//          val end = clock.instant
//          f(req, resp, start, end) onSuccess { 
//            case trackable => client.track(trackable.copy(headers = trackable.headers ++ Map(`User-Agent`.name -> ReportGridUserAgent)))
//          }
//
//          resp
//        }
//      }
//    }
//
//    override val metadata = None
//  }
//}
//

// vim: set ts=4 sw=4 et:
