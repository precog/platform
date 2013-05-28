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
package com.precog.common
package accounts

import security._

import blueeyes.json._

import org.joda.time.DateTime
import scalaz._
import scalaz.syntax.monad._

class TestAccountFinder[M[+_]](accountIds: Map[APIKey, AccountId], accounts: Map[AccountId, Account])(implicit val M: Monad[M])
extends AccountFinder[M] {
  def findAccountByAPIKey(apiKey: APIKey) : M[Option[AccountId]] = M.point(accountIds.get(apiKey))

  def findAccountById(accountId: AccountId): M[Option[Account]] = M.point(accounts.get(accountId))

  def findAccountDetailsById(accountId: AccountId): M[Option[AccountDetails]] = M.point(accounts.get(accountId).map(AccountDetails.from(_)))
}

object TestAccounts {
  def newAccountId() = java.util.UUID.randomUUID.toString.toUpperCase

  def createAccount[M[+_]: Monad](email: String, password: String, creationDate: DateTime, plan: AccountPlan, parentId: Option[AccountId], profile: Option[JValue])(f: AccountId => M[APIKey]): M[Account] = {
    for {
      accountId <- newAccountId().point[M]
      path = Path(accountId)
      apiKey <- f(accountId)
    } yield {
      val salt = Account.randomSalt()
      Account(
        accountId, email,
        Account.saltAndHashSHA256(password, salt), salt,
        creationDate,
        apiKey, path, plan, None, None, profile)
    }
  }
}


// vim: set ts=4 sw=4 et:
