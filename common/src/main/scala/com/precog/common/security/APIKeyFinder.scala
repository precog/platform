package com.precog.common
package security

import service.v1
import accounts.AccountId
import accounts.AccountFinder
import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime
import org.joda.time.Instant

import scalaz._
import scalaz.\/._
import scalaz.std.option.optionInstance
import scalaz.std.list._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.bitraverse._
import scalaz.syntax.std.option._

trait APIKeyFinder[M[+_]] extends AccessControl[M] with Logging { self =>
  def findAPIKey(apiKey: APIKey, rootKey: Option[APIKey]): M[Option[v1.APIKeyDetails]]

  def findAllAPIKeys(fromRoot: APIKey): M[Set[v1.APIKeyDetails]]

  def newAPIKey(accountId: AccountId, path: Path, keyName: Option[String] = None, keyDesc: Option[String] = None): M[v1.APIKeyDetails]

  def addGrant(authKey: APIKey, accountKey: APIKey, grantId: GrantId): M[Boolean]

  def withM[N[+_]](implicit t: M ~> N) = new APIKeyFinder[N] {
    def findAPIKey(apiKey: APIKey, rootKey: Option[APIKey]) =
      t(self.findAPIKey(apiKey, rootKey))

    def findAllAPIKeys(fromRoot: APIKey) =
      t(self.findAllAPIKeys(fromRoot))

    def newAPIKey(accountId: AccountId, path: Path, keyName: Option[String] = None, keyDesc: Option[String] = None) =
      t(self.newAPIKey(accountId, path, keyName, keyDesc))

    def addGrant(authKey: APIKey, accountKey: APIKey, grantId: GrantId) =
      t(self.addGrant(authKey, accountKey, grantId))

    def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]) =
      t(self.hasCapability(apiKey, perms, at))
  }
}

class DirectAPIKeyFinder[M[+_]](underlying: APIKeyManager[M])(implicit val M: Monad[M]) extends APIKeyFinder[M] with Logging {
  val grantDetails: Grant => v1.GrantDetails = {
    case Grant(gid, gname, gdesc, _, _, perms, createdAt, exp) => v1.GrantDetails(gid, gname, gdesc, perms, createdAt, exp)
  }

  def recordDetails(rootKey: Option[APIKey]): PartialFunction[APIKeyRecord, M[v1.APIKeyDetails]] = {
    case APIKeyRecord(apiKey, name, description, issuer, grantIds, false) =>
      underlying.findAPIKeyAncestry(apiKey).flatMap { ancestors =>
        val ancestorKeys = ancestors.drop(1).map(_.apiKey) // The first element of ancestors is the key itself, so we drop it
        grantIds.map(underlying.findGrant).toList.sequence map { grants =>
          val divulgedIssuers = rootKey.map { rk => ancestorKeys.reverse.dropWhile(_ != rk).reverse }.getOrElse(Nil)
          logger.debug("Divulging issuers %s for key %s based on root key %s and ancestors %s".format(divulgedIssuers, apiKey, rootKey, ancestorKeys))
          v1.APIKeyDetails(apiKey, name, description, grants.flatten.map(grantDetails)(collection.breakOut), divulgedIssuers)
        }
      }
  }

  val recordDetails: PartialFunction[APIKeyRecord, M[v1.APIKeyDetails]] = recordDetails(None)

  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]): M[Boolean] = {
    underlying.hasCapability(apiKey, perms, at)
  }

  def findAPIKey(apiKey: APIKey, rootKey: Option[APIKey]) = {
    underlying.findAPIKey(apiKey) flatMap {
      _.collect(recordDetails(rootKey)).sequence
    }
  }

  def findAllAPIKeys(fromRoot: APIKey): M[Set[v1.APIKeyDetails]] = {
    underlying.findAPIKey(fromRoot) flatMap {
      case Some(record) =>
        underlying.findAPIKeyChildren(record.apiKey) flatMap { recs =>
          (recs collect recordDetails).toList.sequence.map(_.toSet)
        }

      case None =>
        M.point(Set())
    }
  }

  def newAPIKey(accountId: AccountId, path: Path, keyName: Option[String] = None, keyDesc: Option[String] = None): M[v1.APIKeyDetails] = {
    underlying.newStandardAPIKeyRecord(accountId, path, keyName, keyDesc) flatMap recordDetails
  }

  def addGrant(authKey: APIKey, accountKey: APIKey, grantId: GrantId): M[Boolean] = {
    underlying.addGrants(accountKey, Set(grantId)) map { _.isDefined }
  }
}
