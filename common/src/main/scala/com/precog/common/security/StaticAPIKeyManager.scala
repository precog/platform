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
