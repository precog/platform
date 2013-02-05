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
package security

import org.joda.time.DateTime

import akka.util.Timeout
import akka.dispatch.{ ExecutionContext, Future, Promise }

import blueeyes.bkka._
import blueeyes.json._
import blueeyes.persistence.mongo._
import blueeyes.json.serialization.Extractor
import blueeyes.json.serialization.DefaultSerialization._

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scalaz._
import scalaz.std.option._

trait StaticAPIKeyManagerComponent {
  implicit def asyncContext: ExecutionContext

  def apiKeyManagerFactory(config: Configuration): APIKeyManager[Future] = {
    new StaticAPIKeyManager(config[String]("masterAccount.apiKey"))
  }
}

class StaticAPIKeyManager(apiKey: APIKey)(implicit val execContext: ExecutionContext) extends APIKeyManager[Future] with Logging {
  logger.info("Starting API Key Manager with root api key: " + apiKey)

  implicit lazy val M: Monad[Future] = AkkaTypeClasses.futureApplicative(execContext)

  private val permissions = Set[Permission](
    ReadPermission(Path("/"), Set.empty[AccountId]),
    DeletePermission(Path("/"), Set.empty[AccountId]),
    ReducePermission(Path("/"), Set.empty[AccountId]),
    WritePermission(Path("/"), Set.empty[AccountId])
  )

  val rootGrant = Grant(java.util.UUID.randomUUID.toString, None, None, Some(apiKey), Set.empty[GrantId], permissions, None)
  val rootAPIKeyRecord = APIKeyRecord(apiKey, Some("Static api key"), None, Some(apiKey), Set(rootGrant.grantId), true)

  val rootGrantId = Promise.successful(rootGrant.grantId)
  val rootAPIKey = Promise.successful(rootAPIKeyRecord.apiKey)

  override def rootPath(apiKey: APIKey): Future[Seq[APIKey]] = Promise.successful(Seq(apiKey))

  def newAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantId]) = sys.error("Static API Key Manager doesn't support modification")
  def newGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantId], perms: Set[Permission], expirationDate: Option[DateTime]) = sys.error("Static API Key Manager doesn't support modification")

  def listAPIKeys() = Promise.successful(Seq(rootAPIKeyRecord))
  def listGrants() = Promise.successful(Seq(rootGrant))

  def findAPIKey(apiKey: APIKey) = Promise.successful(if (apiKey == apiKey) Some(rootAPIKeyRecord) else None)
  def findGrant(grantId: GrantId) = Promise.successful(if (rootGrant.grantId == grantId) Some(rootGrant) else None)
  def findGrantChildren(grantId: GrantId) = Promise.successful(Set.empty)

  def listDeletedAPIKeys() = Promise.successful(Seq())
  def listDeletedGrants() = Promise.successful(Seq())

  def findDeletedAPIKey(apiKey: APIKey) = Promise.successful(None)
  def findDeletedGrant(grantId: GrantId) = Promise.successful(None)
  def findDeletedGrantChildren(grantId: GrantId) = Promise.successful(Set())

  def addGrants(apiKey: APIKey, grants: Set[GrantId]) = sys.error("Static API Key Manager doesn't support modification")
  def removeGrants(apiKey: APIKey, grants: Set[GrantId]) = sys.error("Static API Key Manager doesn't support modification")

  def deleteGrant(grantId: GrantId) = sys.error("Static API Key Manager doesn't support modification")
  def deleteAPIKey(apiKey: APIKey) = sys.error("Static API Key Manager doesn't support modification")

  def close() = Promise.successful(())
}
