package com.precog.common
package security

import com.precog.common.cache.Cache

import java.util.concurrent.TimeUnit._
import org.joda.time.DateTime

import akka.util.Duration

import blueeyes._

import scalaz._
import scalaz.syntax.monad._

case class CachingAPIKeyFinderSettings(
  apiKeyCacheSettings: Seq[Cache.CacheOption],
  grantCacheSettings: Seq[Cache.CacheOption]
)

object CachingAPIKeyFinderSettings {
  val Default = CachingAPIKeyFinderSettings(
    Seq(Cache.ExpireAfterWrite(Duration(5, MINUTES)), Cache.MaxSize(1000)),
    Seq(Cache.ExpireAfterWrite(Duration(5, MINUTES)), Cache.MaxSize(1000))
  )
}

class CachingAPIKeyFinder[M[+_]](manager: APIKeyFinder[M], settings: CachingAPIKeyFinderSettings = CachingAPIKeyFinderSettings.Default)(implicit val M: Monad[M]) extends APIKeyFinder[M] {

  private val apiKeyCache = Cache.simple[APIKey, APIKeyRecord](settings.apiKeyCacheSettings: _*)
  private val grantCache = Cache.simple[GrantId, Grant](settings.grantCacheSettings: _*)

  def findAPIKey(tid: APIKey) = apiKeyCache.get(tid) match {
    case None => manager.findAPIKey(tid).map { _.map { _ ->- add } }
    case t    => M.point(t)
  }
  def findGrant(gid: GrantId) = grantCache.get(gid) match {
    case None        => manager.findGrant(gid).map { _.map { _ ->- add } }
    case s @ Some(_) => M.point(s)
  }

  protected def add(r: APIKeyRecord) = apiKeyCache.put(r.apiKey, r)
  protected def add(g: Grant) = grantCache.put(g.grantId, g)

  protected def remove(r: APIKeyRecord) = apiKeyCache.remove(r.apiKey)
  protected def remove(g: Grant) = grantCache.remove(g.grantId)
}
