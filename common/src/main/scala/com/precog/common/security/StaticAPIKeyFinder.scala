package com.precog.common
package security

import com.precog.common.accounts._
import com.precog.common.security.service._
import org.joda.time.DateTime

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.std.option._
import scalaz.syntax.monad._

class StaticAPIKeyFinder[M[+_]](apiKey: APIKey)(implicit val M: Monad[M]) extends APIKeyFinder[M] with Logging {
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

  def findAllAPIKeys(fromRoot: APIKey): M[Set[v1.APIKeyDetails]] = findAPIKey(fromRoot) map { _.toSet }

  def newAPIKey(accountId: AccountId, path: Path, keyName: Option[String] = None, keyDesc: Option[String] = None): M[v1.APIKeyDetails] = {
    throw new UnsupportedOperationException("API key management unavailable for standalone system.")
  }

  def addGrant(authKey: APIKey, accountKey: APIKey, grantId: GrantId): M[Boolean] = {
    throw new UnsupportedOperationException("Grant management unavailable for standalone system.")
  }

  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]): M[Boolean] = M.point(true)
}