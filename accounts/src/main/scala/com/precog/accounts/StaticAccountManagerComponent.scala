package com.precog
package accounts

import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.bkka.AkkaTypeClasses

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scalaz.Monad

import com.precog.common.security._

trait StaticAccountManagerClientComponent {
  implicit def asyncContext: ExecutionContext

  // For cases where we want to absolutely hard-code the account programmatically
  def hardCodedAccount: Option[String] = None

  def accountManagerFactory(config: Configuration): BasicAccountManager[Future] = {
    hardCodedAccount.orElse(config.get[String]("service.hardcoded_account")).orElse(config.get[String]("service.static_account")).map { accountId =>
      new StaticAccountManager(accountId)(asyncContext)
    }.get
  }
}

class StaticAccountManager(accountId: AccountId)(implicit asyncContext: ExecutionContext)
    extends BasicAccountManager[Future]
    with Logging {
  logger.debug("Starting new static account manager. All queries resolve to \"%s\"".format(accountId))

  implicit lazy val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)

  def listAccountIds(apiKey: APIKey) : Future[Set[AccountId]] = Promise.successful(Set(accountId))

  def mapAccountIds(apiKeys: Set[APIKey]) : Future[Map[APIKey, Set[AccountId]]] = {
    val singleton = Set(accountId)
    Promise.successful {
      apiKeys.map { key => (key, singleton) }.toMap
    }
  }

  def findAccountById(accountId: AccountId): Future[Option[Account]] = Promise.successful(None)

  def close(): Future[Unit] = Promise.successful(())
}
