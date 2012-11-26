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

import scala.collection.mutable

import com.precog.common.Path
import com.precog.common.security._

import org.joda.time.DateTime
import org.bson.types.ObjectId

import scalaz._
import scalaz.syntax.monad._

class InMemoryAccountManager[M[+_]](implicit val M: Monad[M]) extends AccountManager[M] {
  val accounts = new mutable.HashMap[AccountID, Account]
  
  def updateAccount(account: Account): M[Boolean] = {
    findAccountById(account.accountId).map {
      case Some(acct) => accounts.put(account.accountId, account) ; true
      case _ => false
    }
  }

  def newAccountId() = java.util.UUID.randomUUID.toString.toUpperCase
  
  def newAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan, parentId: Option[AccountID])(f: (AccountID, Path) => M[APIKey]): M[Account] = {
    for {
      accountId <- newAccountId().point[M]
      path = Path(accountId)
      apiKey <- f(accountId, path)
    } yield {
      val salt = randomSalt()
      val account = Account(
        accountId, email, 
        saltAndHash(password, salt), salt,
        creationDate,
        apiKey, path, plan)
      accounts.put(accountId, account)
      account
    }
  }

  def listAccountIds(apiKey: APIKey) : M[Set[Account]] = accounts.values.filter(_.apiKey == apiKey).toSet.point[M]
  
  def findAccountById(accountId: AccountID): M[Option[Account]] = accounts.get(accountId).point[M]
  
  def findAccountByEmail(email: String) : M[Option[Account]] = accounts.values.find(_.email == email).point[M]
  
  def deleteAccount(accountId: AccountID): M[Option[Account]] = accounts.remove(accountId).point[M]

  def close(): M[Unit] = ().point[M]
}
