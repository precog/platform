package com.precog.common
package security

import accounts.AccountId
import org.joda.time.DateTime

import akka.util.Timeout
import akka.dispatch.{ ExecutionContext, Future }

//import blueeyes.bkka._
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
  implicit def M: Monad[Future] = new blueeyes.bkka.FutureMonad(asyncContext)

  def apiKeyManagerFactory(config: Configuration): APIKeyManager[Future] = {
    new StaticAPIKeyManager(config[String]("masterAccount.apiKey"))
  }
}

class StaticAPIKeyManager(apiKey: APIKey)(implicit val execContext: ExecutionContext) extends APIKeyManager[Future] with Logging {
  logger.info("Starting API Key Manager with root api key: " + apiKey)

  implicit val M: Monad[Future] = sys.error("todo")//AkkaTypeClasses.futureApplicative(execContext)

  private val permissions = Set[Permission](
    ReadPermission(Path("/"), Set.empty[AccountId]),
    DeletePermission(Path("/"), Set.empty[AccountId])
  )
  
  val rootGrant = Grant(java.util.UUID.randomUUID.toString, None, None, Some(apiKey), Set.empty[GrantId], permissions, None)
  val rootAPIKeyRecord = APIKeyRecord(apiKey, Some("Static api key"), None, Some(apiKey), Set(rootGrant.grantId), true)

  val rootGrantId = Future(rootGrant.grantId)
  val rootAPIKey = Future(rootAPIKeyRecord.apiKey)
  
  def newAPIKey(name: Option[String], description: Option[String], issuerKey: APIKey, grants: Set[GrantId]) = sys.error("Static API Key Manager doesn't support modification")
  def newGrant(name: Option[String], description: Option[String], issuerKey: APIKey, parentIds: Set[GrantId], perms: Set[Permission], expirationDate: Option[DateTime]) = sys.error("Static API Key Manager doesn't support modification") 

  def listAPIKeys() = Future(Seq(rootAPIKeyRecord))
  def listGrants() = Future(Seq(rootGrant))
  
  def findAPIKey(apiKey: APIKey) = Future(if (apiKey == apiKey) Some(rootAPIKeyRecord) else None)
  def findGrant(grantId: GrantId) = Future(if (rootGrant.grantId == grantId) Some(rootGrant) else None)
  def findGrantChildren(grantId: GrantId) = Future(Set.empty)

  def listDeletedAPIKeys() = Future(Seq())
  def listDeletedGrants() = Future(Seq()) 

  def findDeletedAPIKey(apiKey: APIKey) = Future(None)
  def findDeletedGrant(grantId: GrantId) = Future(None)
  def findDeletedGrantChildren(grantId: GrantId) = Future(Set())

  def addGrants(apiKey: APIKey, grants: Set[GrantId]) = sys.error("Static API Key Manager doesn't support modification")
  def removeGrants(apiKey: APIKey, grants: Set[GrantId]) = sys.error("Static API Key Manager doesn't support modification")

  def deleteGrant(grantId: GrantId) = sys.error("Static API Key Manager doesn't support modification")
  def deleteAPIKey(apiKey: APIKey) = sys.error("Static API Key Manager doesn't support modification")

  def close() = Future(())
}
