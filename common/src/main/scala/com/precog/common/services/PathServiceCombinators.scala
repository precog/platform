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
package com.precog.common
package services

import security._

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import com.weiglewilczek.slf4s.Logging
import scalaz.syntax.show._

trait PathServiceCombinators extends HttpRequestHandlerCombinators with Logging {
  def dataPath[A, B, C](prefix: String)(next: HttpService[A, (B, Path) => Future[C]]) = {
    path("""%s/(?:(?<prefixPath>(?:[^\n.](?:[^\n/]|/[^\n])*)/?)?)""".format(prefix)) { 
      new DelegatingService[A, B => Future[C], A, (B, Path) => Future[C]] {
        val delegate = next
        val service = (request: HttpRequest[A]) => {
          logger.debug("Handling dataPath request " + request.shows)
        
          val path: Option[String] = request.parameters.get('prefixPath).filter(_ != null) 
          next.service(request) map { f => (b: B) => f(b, Path(path.getOrElse(""))) }
        }

        val metadata = AboutMetadata(
          PathPatternMetadata(prefix),
          DescriptionMetadata("The portion of the URL path following this prefix will be treated as a path in the Precog virtual filesystem.")
        )
      }
    }
  }
}


// vim: set ts=4 sw=4 et:
