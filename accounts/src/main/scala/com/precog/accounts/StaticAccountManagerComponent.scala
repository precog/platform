package com.precog
package accounts

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka._

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scalaz.Monad

import com.precog.common.security._
import com.precog.common.accounts._

trait StaticAccountManagerClientComponent {
  implicit def asyncContext: ExecutionContext

  // For cases where we want to absolutely hard-code the account programmatically
  def hardCodedAccount: Option[String] = None

  def accountManagerFactory(config: Configuration): AccountFinder[Future] = {
    hardCodedAccount.orElse(config.get[String]("service.hardcoded_account")).orElse(config.get[String]("service.static_account")).map { accountId =>
      new StaticAccountFinder(accountId)(asyncContext)
    }.get
  }
}

class StaticAccountFinder(accountId: AccountId)(implicit asyncContext: ExecutionContext)
    extends AccountFinder[Future]
    with Logging {
  logger.debug("Starting new static account manager. All queries resolve to \"%s\"".format(accountId))

  implicit lazy val M: Monad[Future] = new FutureMonad(asyncContext)

  def findAccountByAPIKey(apiKey: APIKey) : Future[Option[AccountId]] = Promise.successful(Some(accountId))

  def findAccountById(accountId: AccountId): Future[Option[Account]] = Promise.successful(None)

  def close(): Future[Unit] = Promise.successful(())
}
