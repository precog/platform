package com.precog
package accounts

import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.security._
import com.precog.util.PrecogUnit

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

  def resetAccountPassword(accountId: AccountId, tokenId: ResetTokenId, newPassword: String): M[Boolean] = {
    findAccountByResetToken(accountId, tokenId).flatMap {
      _.map { account =>
        for {
          updated <- updateAccountPassword(account, newPassword)
          _       <- markResetTokenUsed(tokenId)
        } yield updated
      }.getOrElse(M.point(false))
    }
  }

  def generateResetToken(accountId: Account): M[ResetTokenId]

  def markResetTokenUsed(tokenId: ResetTokenId): M[PrecogUnit]

  def findResetToken(accountId: AccountId, tokenId: ResetTokenId): M[Option[ResetToken]]

  // The accountId is used here as a sanity/security check only, not for lookup
  def findAccountByResetToken(accountId: AccountId, tokenId: ResetTokenId): M[Option[Account]] = {
    logger.debug("Locating account for token id %s, account id %s".format(tokenId, accountId))
    findResetToken(accountId, tokenId).flatMap {
      case Some(token) =>
        if (token.expiresAt.isBefore(new DateTime)) {
          logger.warn("Located expired reset token: " + token)
          M.point(None)
        } else if (token.usedAt.nonEmpty) {
          logger.warn("Reset attempted with previously used reset token: " + token)
          M.point(None)
        } else if (token.accountId != accountId) {
          logger.debug("Located reset token, but with the wrong account (expected %s): %s".format(accountId, token))
          M.point(None)
        } else {
          logger.debug("Located reset token " + token)
          findAccountById(token.accountId)
        }

      case None =>
        logger.warn("Could not locate reset token for id " + tokenId)
        M.point(None)
    }
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
