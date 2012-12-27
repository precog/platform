package com.precog.accounts

import scala.collection.mutable

import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.security._

import org.joda.time.DateTime
import org.bson.types.ObjectId

import scalaz._
import scalaz.syntax.monad._

class InMemoryAccountManager[M[+_]](implicit val M: Monad[M]) extends AccountManager[M] {
  val accounts = new mutable.HashMap[AccountId, Account]
  
  def updateAccount(account: Account): M[Boolean] = {
    findAccountById(account.accountId).map {
      case Some(acct) => accounts.put(account.accountId, account) ; true
      case _ => false
    }
  }

  def newAccountId() = java.util.UUID.randomUUID.toString.toUpperCase
  
  def newAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan, parentId: Option[AccountId])(f: (AccountId, Path) => M[APIKey]): M[Account] = {
    for {
      accountId <- newAccountId().point[M]
      path = Path(accountId)
      apiKey <- f(accountId, path)
    } yield {
      val salt = randomSalt()
      val account = Account(
        accountId, email, 
        saltAndHashSHA256(password, salt), salt,
        creationDate,
        apiKey, path, plan)
      accounts.put(accountId, account)
      account
    }
  }

  def findAccountByAPIKey(apiKey: APIKey) : M[Option[AccountId]] = {
    accounts.values.find(_.apiKey == apiKey).map(_.accountId).point[M]
  }

  def findAccountById(accountId: AccountId): M[Option[Account]] = accounts.get(accountId).point[M]
  
  def findAccountByEmail(email: String) : M[Option[Account]] = accounts.values.find(_.email == email).point[M]
  
  def deleteAccount(accountId: AccountId): M[Option[Account]] = accounts.remove(accountId).point[M]
}
