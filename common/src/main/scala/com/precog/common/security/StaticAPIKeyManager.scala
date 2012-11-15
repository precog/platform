package com.precog.common
package security

import org.joda.time.DateTime

import akka.util.Timeout
import akka.dispatch.{ ExecutionContext, Future }

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
  implicit lazy val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)

  def apiKeyManagerFactory(config: Configuration): APIKeyManager[Future] = {
    new StaticAPIKeyManager(config[String]("masterAccount.apiKey"))
  }
}

class StaticAPIKeyManager(apiKey: APIKey)(implicit val execContext: ExecutionContext) extends APIKeyManager[Future] with Logging {
  logger.info("Starting API Key Manager with root api key: " + apiKey)

  implicit lazy val M: Monad[Future] = AkkaTypeClasses.futureApplicative(execContext)

  private val permissions = Set[Permission](
    ReadPermission(Path("/"), Set.empty[AccountID]),
    DeletePermission(Path("/"), Set.empty[AccountID])
  )
  
  val rootGrant = Grant(java.util.UUID.randomUUID.toString, None, None, Some(apiKey), Set.empty[GrantID], permissions, None)
  val rootAPIKeyRecord = APIKeyRecord(apiKey, Some("Static api key"), None, Some(apiKey), Set(rootGrant.grantId), true)

  val rootGrantId = Future(rootGrant.grantId)
  val rootAPIKey = Future(rootAPIKeyRecord.apiKey)
  
  def newAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantID]) = sys.error("Static API Key Manager doesn't support modification")
  def newGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantID], perms: Set[Permission], expirationDate: Option[DateTime]) = sys.error("Static API Key Manager doesn't support modification") 

  def listAPIKeys() = Future(Seq(rootAPIKeyRecord))
  def listGrants() = Future(Seq(rootGrant))
  
  def findAPIKey(apiKey: APIKey) = Future(if (apiKey == apiKey) Some(rootAPIKeyRecord) else None)
  def findGrant(grantId: GrantID) = Future(if (rootGrant.grantId == grantId) Some(rootGrant) else None)
  def findGrantChildren(grantId: GrantID) = Future(Set.empty)

  def listDeletedAPIKeys() = Future(Seq())
  def listDeletedGrants() = Future(Seq()) 

  def findDeletedAPIKey(apiKey: APIKey) = Future(None)
  def findDeletedGrant(grantId: GrantID) = Future(None)
  def findDeletedGrantChildren(grantId: GrantID) = Future(Set())

  def addGrants(apiKey: APIKey, grants: Set[GrantID]) = sys.error("Static API Key Manager doesn't support modification")
  def removeGrants(apiKey: APIKey, grants: Set[GrantID]) = sys.error("Static API Key Manager doesn't support modification")

  def deleteGrant(grantId: GrantID) = sys.error("Static API Key Manager doesn't support modification")
  def deleteAPIKey(apiKey: APIKey) = sys.error("Static API Key Manager doesn't support modification")

  def close() = Future(())
}
