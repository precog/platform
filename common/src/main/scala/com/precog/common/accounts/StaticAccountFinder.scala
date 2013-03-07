package com.precog.common
package accounts

import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import org.streum.configrity.Configuration

import scalaz.Monad
import scalaz.syntax.monad._

import com.precog.common._
import com.precog.common.security._

class StaticAccountFinder[M[+_]: Monad](apiKey: APIKey, accountId: AccountId) extends AccountFinder[M] with Logging {
  logger.debug("Constructed new static account manager. All queries resolve to \"%s\"".format(accountId))

  val details = Some(AccountDetails(accountId, "no email", new DateTime(0), apiKey, Path("/"), AccountPlan.Root))

  def findAccountByAPIKey(apiKey: APIKey) : M[Option[AccountId]] = Some(accountId).point[M]

  def findAccountDetailsById(accountId: AccountId): M[Option[AccountDetails]] = details.point[M]
}
