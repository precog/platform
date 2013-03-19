package com.precog.common
package accounts

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import com.precog.common.Path

import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import org.streum.configrity.Configuration

import scalaz.Monad
import scalaz.syntax.monad._

import com.precog.common.security._

class StaticAccountFinder[M[+_]: Monad](accountId: AccountId, apiKey: APIKey, rootPath: Option[String] = None, email: String = "static@precog.com") extends AccountFinder[M] with Logging {
  logger.debug("Constructed new static account manager. All queries resolve to \"%s\"".format(accountId))

  private[this] val details = Some(AccountDetails(accountId, email, new DateTime(0), apiKey, Path(rootPath.getOrElse("/" + accountId)), AccountPlan.Free))

  def findAccountByAPIKey(apiKey: APIKey) = Some(accountId).point[M]

  def findAccountDetailsById(accountId: AccountId) = details.point[M]
}
