package com.precog.common
package security

import service.v1
import accounts.AccountId
import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import scalaz._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._

trait APIKeyFinder[M[+_]] extends AccessControl[M] with Logging { self =>
  def findAPIKey(apiKey: APIKey): M[Option[v1.APIKeyDetails]]

  def findAllAPIKeys(fromRoot: APIKey): M[Set[v1.APIKeyDetails]]

  def newAPIKey(accountId: AccountId, path: Path, keyName: Option[String] = None, keyDesc: Option[String] = None): M[v1.APIKeyDetails]

  def addGrant(authKey: APIKey, accountKey: APIKey, grantId: GrantId): M[Boolean]

  def withM[N[+_]](implicit t: M ~> N) = new APIKeyFinder[N] {
    def findAPIKey(apiKey: APIKey) =
      t(self.findAPIKey(apiKey))

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

class DirectAPIKeyFinder[M[+_]](val underlying: APIKeyManager[M])(implicit val M: Monad[M]) extends APIKeyFinder[M] {
  val grantDetails: Grant => v1.GrantDetails = {
    case Grant(gid, gname, gdesc, _, _, perms, exp) => v1.GrantDetails(gid, gname, gdesc, perms, exp)
  }

  val recordDetails: PartialFunction[APIKeyRecord, M[v1.APIKeyDetails]] = {
    case APIKeyRecord(apiKey, name, description, _, grantIds, false) =>
      grantIds.map(underlying.findGrant).sequence map { grants =>
        v1.APIKeyDetails(apiKey, name, description, grants.flatten map grantDetails)
      }
  }

  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]): M[Boolean] = {
    underlying.hasCapability(apiKey, perms, at)
  }

  def findAPIKey(apiKey: APIKey) = {
    underlying.findAPIKey(apiKey) flatMap {
      _ collect recordDetails sequence
    }
  }

  def findAllAPIKeys(fromRoot: APIKey): M[Set[v1.APIKeyDetails]] = {
    underlying.findAPIKey(fromRoot) flatMap {
      case Some(record) =>
        underlying.findAPIKeyChildren(record.apiKey) flatMap {
          _ collect recordDetails sequence
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


// vim: set ts=4 sw=4 et:
