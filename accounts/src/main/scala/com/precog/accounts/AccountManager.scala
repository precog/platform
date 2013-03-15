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

import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.security._

import org.joda.time.DateTime

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.std.stream._

trait AccountManager[M[+_]] extends AccountFinder[M] {
  import Account._

  implicit def M: Monad[M]

  def findAccountById(accountId: AccountId): M[Option[Account]]

  def findAccountDetailsById(accountId: AccountId): M[Option[AccountDetails]] = 
    findAccountById(accountId).map(_.map(AccountDetails.from(_)))

  def updateAccount(account: Account): M[Boolean]

  def updateAccountPassword(account: Account, newPassword: String): M[Boolean] = {
    val salt = randomSalt()
    updateAccount(account.copy(passwordHash = saltAndHashSHA256(newPassword, salt), passwordSalt = salt, lastPasswordChangeTime = Some(new DateTime)))
  }

  def newAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan, parentId: Option[AccountId] = None)(f: (AccountId, Path) => M[APIKey]): M[Account]

  def findAccountByEmail(email: String) : M[Option[Account]]

  def hasAncestor(child: Account, ancestor: Account)(implicit M: Monad[M]): M[Boolean] = {
    if (child == ancestor) {
      true.point[M]
    } else {
      child.parentId map { id =>
        findAccountById(id) flatMap {
          case None => false.point[M]
          case Some(`child`) => false.point[M] // avoid infinite loops
          case Some(parent) => hasAncestor(parent, ancestor) 
        }
      } getOrElse {
        false.point[M]
      }
    }
  }

  def authAccount(email: String, password: String)(implicit M: Monad[M]): M[Validation[String, Account]] = {
    findAccountByEmail(email) map {
      case Some(account) if account.passwordHash == saltAndHashSHA1(password, account.passwordSalt) ||
          account.passwordHash == saltAndHashSHA256(password, account.passwordSalt) ||
          account.passwordHash == saltAndHashLegacy(password, account.passwordSalt) => Success(account)
      case Some(account) => Failure("password mismatch")
      case None          => Failure("account not found")
    }
  }

  def deleteAccount(accountId: AccountId): M[Option[Account]]
}
