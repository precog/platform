package com.precog.common
package accounts

import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import org.streum.configrity.Configuration

import scalaz.Monad
import scalaz.syntax.monad._

import com.precog.common._
import com.precog.common.security._

class StaticAccountFinder[M[+_]: Monad](accountId: AccountId, apiKey: APIKey, rootPath: Option[String] = None, email: String = "static@precog.com") extends AccountFinder[M] with Logging {
  private[this] val details = Some(AccountDetails(accountId, email, new DateTime(0), apiKey, Path(rootPath.getOrElse("/" + accountId)), AccountPlan.Root))

  logger.debug("Constructed new static account manager. All queries resolve to \"%s\"".format(details.get))

  def findAccountByAPIKey(apiKey: APIKey) = Some(accountId).point[M]

  def findAccountDetailsById(accountId: AccountId) = details.point[M]
}
