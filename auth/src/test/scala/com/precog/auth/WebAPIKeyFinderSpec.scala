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
package com.precog.auth

import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.client._

import blueeyes.core.data._
import blueeyes.core.service._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.bkka._

import akka.util.Duration
import akka.dispatch._

import org.streum.configrity.Configuration

import scalaz._
import scalaz.syntax.comonad._

class WebAPIKeyFinderSpec extends APIKeyFinderSpec[Future] with AkkaDefaults { self =>
  implicit lazy val executionContext = defaultFutureDispatch
  implicit lazy val M: Monad[Future] with Comonad[Future] = new UnsafeFutureComonad(executionContext, Duration(5, "seconds"))

  def withAPIKeyFinder[A](mgr: APIKeyManager[Future])(f: APIKeyFinder[Future] => A): A = {
    val testService = new TestAPIKeyService {
      val apiKeyManager = mgr
    }
    val (service, stoppable) = testService.server(Configuration.parse(testService.config), executionContext).start.get.copoint
    val client = new HttpClient[ByteChunk] {
      def isDefinedAt(req: HttpRequest[ByteChunk]) = true
      def apply(req: HttpRequest[ByteChunk]) =
        service.service(req) getOrElse Future(HttpResponse(HttpStatus(NotFound)))
    }

    val apiKeyFinder = new WebAPIKeyFinder {
      val M = self.M
      val rootAPIKey = self.M.copoint(mgr.rootAPIKey)
      val rootGrantId = self.M.copoint(mgr.rootGrantId)
      val executor = self.executionContext
      protected def withRawClient[A](f: HttpClient[ByteChunk] => A): A = f(client.path("/security/v1/"))
    }

    val result = f(apiKeyFinder.withM[Future])

    stoppable foreach { stop =>
      Stoppable.stop(stop, Duration(1, "minutes")).copoint
    }
    result
  }
}
