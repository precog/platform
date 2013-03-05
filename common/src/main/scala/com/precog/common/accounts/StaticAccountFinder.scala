package com.precog.common
package accounts

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import com.weiglewilczek.slf4s.Logging

import org.joda.time.DateTime

import org.streum.configrity.Configuration

import scalaz.Monad

import com.precog.common._
import com.precog.common.security._

class StaticAccountFinder(apiKey: APIKey, accountId: AccountId)(implicit executor: ExecutionContext) extends AccountFinder[Future] with Logging {
  logger.debug("Constructed new static account manager. All queries resolve to \"%s\"".format(accountId))

  val details = Some(AccountDetails(accountId, "no email", new DateTime(0), apiKey, Path("/"), AccountPlan.Root))

  implicit val M: Monad[Future] = new FutureMonad(executor)

  def findAccountByAPIKey(apiKey: APIKey) : Future[Option[AccountId]] = Promise.successful(Some(accountId))

  def findAccountDetailsById(accountId: AccountId): Future[Option[AccountDetails]] = Promise.successful(details)
}
