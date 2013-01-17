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
package com.precog.mongo

import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.security._
import com.precog.common.security.service._
import org.joda.time.DateTime

//import blueeyes.bkka._
import blueeyes.json._
import blueeyes.persistence.mongo._
import blueeyes.json.serialization.Extractor
import blueeyes.json.serialization.DefaultSerialization._

import akka.util.Timeout
import akka.dispatch.{ ExecutionContext, Future }

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scalaz._
import scalaz.std.option._

class StaticAPIKeyFinder(apiKey: APIKey)(implicit val M: Monad[Future]) extends APIKeyFinder[Future] with Logging {
  private val permissions = Set[Permission](
    ReadPermission(Path("/"), Set.empty[AccountId]),
    DeletePermission(Path("/"), Set.empty[AccountId])
  )
  
  val rootGrant = v1.GrantDetails(java.util.UUID.randomUUID.toString, None, None, permissions, None)
  val rootAPIKeyRecord = v1.APIKeyDetails(apiKey, Some("Static api key"), None, Set(rootGrant))

  val rootGrantId = M.point(rootGrant.grantId)
  val rootAPIKey = M.point(rootAPIKeyRecord.apiKey)
  
  def findAPIKey(apiKey: APIKey) = M.point(if (apiKey == apiKey) Some(rootAPIKeyRecord) else None)
  def findGrant(grantId: GrantId) = M.point(if (rootGrant.grantId == grantId) Some(rootGrant) else None)

  def findAllAPIKeys(fromRoot: APIKey): Future[Set[v1.APIKeyDetails]] = findAPIKey(fromRoot) map { _.toSet }

  def newAPIKey(accountId: AccountId, path: Path, keyName: Option[String] = None, keyDesc: Option[String] = None): Future[v1.APIKeyDetails] = {
    throw new UnsupportedOperationException("API key management unavailable for standalone system.")
  }

  def addGrant(authKey: APIKey, accountKey: APIKey, grantId: GrantId): Future[Boolean] = {
    throw new UnsupportedOperationException("Grant management unavailable for standalone system.")
  }

  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]): Future[Boolean] = M.point(true)
}
