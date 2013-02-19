package com.precog.common
package accounts

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scalaz.Monad

import com.precog.common.security._

class StaticAccountFinder(accountId: AccountId)(implicit executor: ExecutionContext) extends AccountFinder[Future] with Logging {
  logger.debug("Constructed new static account manager. All queries resolve to \"%s\"".format(accountId))

  implicit val M: Monad[Future] = new FutureMonad(executor)

  def findAccountByAPIKey(apiKey: APIKey) : Future[Option[AccountId]] = Promise.successful(Some(accountId))

  def findAccountById(accountId: AccountId): Future[Option[Account]] = Promise.successful(None)
}
