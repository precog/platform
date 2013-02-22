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
