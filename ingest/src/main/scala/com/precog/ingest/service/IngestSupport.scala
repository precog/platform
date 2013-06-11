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

import org.joda.time.Instant

import com.precog.common.Path
import com.precog.common.security._

import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.util.Clock

import akka.dispatch.Future

import scalaz._
import scalaz.syntax.std.boolean._
import scalaz.syntax.show._
import scalaz.syntax.monad._

import com.weiglewilczek.slf4s._

trait IngestSupport extends Logging {
  def permissionsFinder: PermissionsFinder[Future]
  def clock: Clock
  implicit def M: Monad[Future]
  
  // Ideally, this would just be a combinator. However, FileStoreHandler can't use the path
  // as-is, so the path we get here may need to be manged first, so we need to address that
  // first and my plate is full up of other things to do.

  def findRequestWriteAuthorities[A](request: HttpRequest[A], apiKey: APIKey, path: Path, timestampO: Option[Instant] = None)(f: Authorities => Future[HttpResponse[JValue]]): Future[HttpResponse[JValue]] = {
    val timestamp = timestampO getOrElse clock.now().toInstant
    val requestAuthorities = for {
      paramIds <- request.parameters.get('ownerAccountId)
      ids = paramIds.split("""\s*,\s*""")
      auths <- Authorities.ifPresent(ids.toSet) if ids.nonEmpty
    } yield auths

    requestAuthorities map { authorities =>
      permissionsFinder.checkWriteAuthorities(authorities, apiKey, path, timestamp.toInstant) map { _.option(authorities) }
    } getOrElse {
      permissionsFinder.inferWriteAuthorities(apiKey, path, Some(timestamp.toInstant))
    } onFailure {
      case ex: Exception =>
        logger.error("Request " + request.shows + " failed due to unavailability of security subsystem.", ex)
        // FIXME: Provisionally accept data for ingest if one of the permissions-checking services is unavailable
    } flatMap {
      case Some(authorities) =>
        f(authorities)

      case None =>
        logger.warn("Unable to resolve accounts for write from %s owners %s to path %s".format(apiKey, request.parameters.get('ownerAccountId), path))
        M.point(HttpResponse[JValue](Forbidden, content = Some(JString("Either the ownerAccountId parameter you specified could not be resolved to a set of permitted accounts, or the API key specified was invalid."))))
    }
  }
}
