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
package com.precog.accounts

import scala.collection.mutable

import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.security._
import com.precog.util.PrecogUnit

import blueeyes.json._

import org.joda.time.DateTime
import org.bson.types.ObjectId

import scalaz._
import scalaz.syntax.monad._

class InMemoryAccountManager[M[+_]](resetExpiration: Int = 1)(implicit val M: Monad[M]) extends AccountManager[M] {
  val accounts = new mutable.HashMap[AccountId, Account]
  val resetTokens = new mutable.HashMap[ResetTokenId, ResetToken]

  def updateAccount(account: Account): M[Boolean] = {
    findAccountById(account.accountId).map {
      case Some(acct) => accounts.put(account.accountId, account) ; true
      case _ => false
    }
  }

  def createAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan, parentId: Option[AccountId], profile: Option[JValue])(f: AccountId => M[APIKey]): M[Account] = {
    TestAccounts.createAccount(email, password, creationDate, plan, parentId, profile)(f) map { account =>
      accounts.put(account.accountId, account)
      account
    }
  }

  def setAccount(accountId: AccountId, email: String, password: String, creationDate: DateTime, plan: AccountPlan, parentId: Option[AccountId]) = {
    createAccount(email, password, creationDate, plan, parentId, None) {
      _ => M.point(java.util.UUID.randomUUID().toString.toLowerCase)
    } map {
      account => {
        accounts -= account.accountId
        accounts += (accountId -> account.copy(accountId = accountId))
      }
    }
  }

  def findAccountByAPIKey(apiKey: APIKey) : M[Option[AccountId]] = {
    accounts.values.find(_.apiKey == apiKey).map(_.accountId).point[M]
  }

  def findAccountById(accountId: AccountId): M[Option[Account]] = accounts.get(accountId).point[M]

  def findAccountByEmail(email: String) : M[Option[Account]] = accounts.values.find(_.email == email).point[M]

  def findResetToken(accountId: AccountId, tokenId: ResetTokenId): M[Option[ResetToken]] = {
    M.point(resetTokens.get(tokenId))
  }

  def generateResetToken(account: Account, expiration: DateTime): M[ResetTokenId] = {
    val tokenId = java.util.UUID.randomUUID.toString.replace("-","")

    val token = ResetToken(tokenId, account.accountId, account.email, expiration)

    resetTokens += (tokenId -> token)

    M.point(tokenId)
  }

  def markResetTokenUsed(tokenId: ResetTokenId): M[PrecogUnit] = M.point {
    resetTokens.get(tokenId).foreach {
      token => resetTokens += (tokenId -> token.copy(usedAt = Some(new DateTime)))
    }
    PrecogUnit
  }

  def generateResetToken(account: Account): M[ResetTokenId] = generateResetToken(account, (new DateTime).plusMinutes(resetExpiration))

  def deleteAccount(accountId: AccountId): M[Option[Account]] = accounts.remove(accountId).point[M]
}
