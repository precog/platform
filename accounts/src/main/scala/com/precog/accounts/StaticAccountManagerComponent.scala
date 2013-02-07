/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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
