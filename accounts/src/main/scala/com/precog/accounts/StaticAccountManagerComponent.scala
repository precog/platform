package com.precog
package accounts

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scalaz.Monad

import com.precog.common.security._
import com.precog.common.accounts._

class StaticAccountFinder(accountId: AccountId)(implicit asyncContext: ExecutionContext) extends AccountFinder[Future] with Logging {
  logger.debug("Starting new static account manager. All queries resolve to \"%s\"".format(accountId))

  implicit val M: Monad[Future] = new FutureMonad(asyncContext)

  def findAccountByAPIKey(apiKey: APIKey) : Future[Option[AccountId]] = Promise.successful(Some(accountId))

  def findAccountById(accountId: AccountId): Future[Option[Account]] = Promise.successful(None)
}
