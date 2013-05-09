package com.precog.common
package security

import service.v1
import accounts.AccountId
import com.precog.util.cache.Cache

import akka.util.Duration

import java.util.concurrent.TimeUnit._
import org.joda.time.DateTime

import scalaz._
import scalaz.effect._
import scalaz.syntax.monad._
import scalaz.syntax.effect.id._

case class CachingAPIKeyFinderSettings(
  apiKeyCacheSettings: Seq[Cache.CacheOption[APIKey, v1.APIKeyDetails]],
  grantCacheSettings: Seq[Cache.CacheOption[APIKey, v1.APIKeyDetails]]
)

object CachingAPIKeyFinderSettings {
  val Default = CachingAPIKeyFinderSettings(
    Seq(Cache.ExpireAfterWrite(Duration(5, MINUTES)), Cache.MaxSize(1000)),
    Seq(Cache.ExpireAfterWrite(Duration(5, MINUTES)), Cache.MaxSize(1000))
  )
}

class CachingAPIKeyFinder[M[+_]](delegate: APIKeyFinder[M], settings: CachingAPIKeyFinderSettings = CachingAPIKeyFinderSettings.Default)(implicit val M: Monad[M]) extends APIKeyFinder[M] {
  private val apiKeyCache = Cache.simple[APIKey, v1.APIKeyDetails](settings.apiKeyCacheSettings: _*)

  def findAPIKey(tid: APIKey, rootKey: Option[APIKey]) = apiKeyCache.get(tid) match {
    case None => delegate.findAPIKey(tid, rootKey).map { _ map { _ tap add unsafePerformIO } }
    case t    => M.point(t)
  }

  def findAllAPIKeys(fromRoot: APIKey): M[Set[v1.APIKeyDetails]] = {
    sys.error("todo")
  }

  protected def add(r: v1.APIKeyDetails) = IO { apiKeyCache.put(r.apiKey, r) }

  protected def remove(r: v1.APIKeyDetails) = IO { apiKeyCache.remove(r.apiKey) } 

  // TODO: Cache capability checks
  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]): M[Boolean] = 
    delegate.hasCapability(apiKey, perms, at)

  // TODO: Cache on creation
  def createAPIKey(accountId: AccountId, keyName: Option[String] = None, keyDesc: Option[String] = None): M[v1.APIKeyDetails] = 
    delegate.createAPIKey(accountId, keyName, keyDesc)

  def addGrant(accountKey: APIKey, grantId: GrantId): M[Boolean] = 
    delegate.addGrant(accountKey, grantId)
}
