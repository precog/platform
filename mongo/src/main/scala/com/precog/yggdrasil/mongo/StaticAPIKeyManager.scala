package com.precog.mongo

import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.security._
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
  
  val rootGrant = Grant(java.util.UUID.randomUUID.toString, None, None, Some(apiKey), Set.empty[GrantId], permissions, None)
  val rootAPIKeyRecord = APIKeyRecord(apiKey, Some("Static api key"), None, Some(apiKey), Set(rootGrant.grantId), true)

  val rootGrantId = M.point(rootGrant.grantId)
  val rootAPIKey = M.point(rootAPIKeyRecord.apiKey)
  
  def findAPIKey(apiKey: APIKey) = M.point(if (apiKey == apiKey) Some(rootAPIKeyRecord) else None)
  def findGrant(grantId: GrantId) = M.point(if (rootGrant.grantId == grantId) Some(rootGrant) else None)
}
