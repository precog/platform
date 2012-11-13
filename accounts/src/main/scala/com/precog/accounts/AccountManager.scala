package com.precog
package accounts

import com.precog.common.Path
import com.precog.common.security._

import org.joda.time.DateTime

import com.google.common.base.Charsets
import com.google.common.hash.Hashing

import scalaz._
import scalaz.syntax.monad._

trait AccountManager[M[+_]] {
  implicit val M: Monad[M]
  
  private val randomSource = new java.security.SecureRandom

  def randomSalt() = {
    val saltBytes = new Array[Byte](256)
    randomSource.nextBytes(saltBytes)
    saltBytes.flatMap(byte => Integer.toHexString(0xFF & byte))(collection.breakOut) : String
  }

  def saltAndHash(password: String, salt: String): String = {
    Hashing.sha1().hashString(password + salt, Charsets.UTF_8).toString
  }
  
  def updateAccount(account: Account): M[Boolean]
  
  def updateAccountPassword(account: Account, newPassword: String): M[Boolean] = {
    val salt = randomSalt()
    updateAccount(account.copy(passwordHash = saltAndHash(newPassword, salt), passwordSalt = salt))
  }
 
  def newAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan)(f: (AccountID, Path) => M[APIKey]): M[Account]

  def listAccountIds(apiKey: APIKey) : M[Set[Account]]
  
  def findAccountById(accountId: AccountID): M[Option[Account]]
  def findAccountByEmail(email: String) : M[Option[Account]]

  def authAccount(email: String, password: String) = {
    for {
      accountOpt <- findAccountByEmail(email)
    } yield {
      accountOpt filter { account =>
        account.passwordHash == saltAndHash(password, account.passwordSalt)
      }
    }
  }

  
  def deleteAccount(accountId: AccountID): M[Option[Account]]

  def close(): M[Unit]
} 
