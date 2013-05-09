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
