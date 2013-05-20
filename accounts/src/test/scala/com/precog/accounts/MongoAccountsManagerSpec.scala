package com.precog.accounts

import com.precog.common.accounts._

import org.specs2.execute.Result
import org.specs2.mutable.{After, Specification}
import org.specs2.specification._

import akka.actor.ActorSystem
import akka.util.Duration
import akka.util.Timeout
import akka.dispatch.Future
import akka.dispatch.Await
import akka.dispatch.ExecutionContext

import blueeyes.bkka.UnsafeFutureComonad
import blueeyes.persistence.mongo._

import blueeyes.json._

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat

import org.streum.configrity._

import scalaz._
import scalaz.syntax.comonad._

object MongoAccountManagerSpec extends Specification with RealMongoSpecSupport {
  val timeout = Duration(30, "seconds")

  "MongoAccountManager" should {
    "find an Account by accountId" in new AccountManager {
      accountManager.findAccountById(account.accountId).copoint must beLike {
        case Some(Account(accountId,_,_,_,_,_,_,_,_,_,_)) => accountId must_== account.accountId
      }
    }

     "find an Account by email address" in new AccountManager {
       accountManager.findAccountByEmail(account.email).copoint must beLike {
        case Some(Account(accountId,_,_,_,_,_,_,_,_,_,_)) => accountId must_== account.accountId
      }
    }

    "not find a non-existent Account" in new AccountManager {
      accountManager.findAccountById(notFoundAccountId).copoint must beLike {
        case None => ok
      }
    }

    "update an Account" in new AccountManager {
      val updatedAccount = account.copy(apiKey = "new API key")

      (for {
        _ <- accountManager.updateAccount(updatedAccount)
        result2 <- accountManager.findAccountById(account.accountId)
      } yield result2).copoint must beLike {
        case Some(Account(accountId,_,_,_,_,apiKey,_,_,_,_,_)) => apiKey must_== updatedAccount.apiKey
      }
    }

    "move an Account to the deleted collection" in new AccountManager {
      type Results = (Option[Account], Option[Account], Option[Account])//, Option[Account])

      (for {
        before <- accountManager.findAccountById(account.accountId)
        deleted <- accountManager.deleteAccount(before.get.accountId)
        after <- accountManager.findAccountById(account.accountId)
       // deleteCol <- accountManager.findDeletedAccount(account.accountId)
      } yield {
        (before, deleted, after)
      }).copoint must beLike {
        case (Some(t1), Some(t2), None) =>
          t1 must_== t2
          //t1 must_== t3
      }
    }

    "succeed in deleting a previously deleted Account" in new AccountManager {
      type Results = (Option[Account], Option[Account], Option[Account], Option[Account])//, Option[Account])

      (for {
        before <- accountManager.findAccountById(account.accountId)
        deleted1 <- accountManager.deleteAccount(before.get.accountId)
        deleted2 <- accountManager.deleteAccount(before.get.accountId)
        after <- accountManager.findAccountById(account.accountId)
      //  deleteCol <- accountManager.findDeletedAccount(account.accountId)
      } yield {
        (before, deleted1, deleted2, after)
      }).copoint must beLike {
        case (Some(t1), Some(t2), None, None) =>
          t1 must_== t2
          //t1 must_== t4
      }
    }

    "properly generate and retrieve a reset token" in new AccountManager {
      (for {
        tokenId <- accountManager.generateResetToken(account)
        resolvedAccount <- accountManager.findAccountByResetToken(account.accountId, tokenId)
      } yield resolvedAccount).copoint must beLike {
        case \/-(resolvedAccount) =>
          resolvedAccount.accountId must_== account.accountId
      }
    }

    "not locate expired password reset tokens" in new AccountManager {
      (for {
        tokenId <- accountManager.generateResetToken(account, (new DateTime).minusMinutes(5))
        resolvedAccount <- accountManager.findAccountByResetToken(account.accountId, tokenId)
      } yield resolvedAccount).copoint must beLike {
        case -\/(_) => ok
      }
    }

    "update an Account password with a reset token" in new AccountManager {
      val newPassword = "bluemeanies"
      (for {
        tokenId    <- accountManager.generateResetToken(account)
        _          <- accountManager.resetAccountPassword(account.accountId, tokenId, newPassword)
        authResultBad  <- accountManager.authAccount(account.email, origPassword)
        authResultGood <- accountManager.authAccount(account.email, newPassword)
      } yield (authResultBad, authResultGood)).copoint must beLike {
        case (Failure("password mismatch"), Success(authenticated)) => authenticated.accountId must_== account.accountId
      }
    }

    "not update an Account password with a previously used reset token" in new AccountManager {
      val newPassword = "bluemeanies"
      val newPassword2 = "notreally"

      (for {
        tokenId    <- accountManager.generateResetToken(account)
        _          <- accountManager.resetAccountPassword(account.accountId, tokenId, newPassword)
        _          <- accountManager.resetAccountPassword(account.accountId, tokenId, newPassword2)
        // We should still be able to authenticate with the *first* changed password
        authResult <- accountManager.authAccount(account.email, newPassword)
      } yield authResult).copoint must beLike[Validation[String, Account]] {
        case Success(authenticated) => authenticated.accountId must_== account.accountId
      }
    }

  }

  object Counter {
    val cnt = new AtomicLong
  }

  val dbInstance = new AtomicInteger

  val defaultSettings = new MongoAccountManagerSettings {
    def accounts: String = "accounts"
    def deletedAccounts: String = "deleted_accounts"
    def timeout: Timeout = new Timeout(30000)

    def resetTokens: String = "reset_tokens"
    def resetTokenExpirationMinutes: Int = 30
  }

  class AccountManager extends After {
    val defaultActorSystem = ActorSystem("AccountManagerTest")
    implicit val execContext = ExecutionContext.defaultExecutionContext(defaultActorSystem)
    implicit val M = new UnsafeFutureComonad(execContext, Duration(60, "seconds"))

    val accountManager = new MongoAccountManager(mongo, mongo.database("test_v1_" + dbInstance.getAndIncrement), defaultSettings) {
      def newAccountId = M.point("test%04d".format(dbInstance.getAndIncrement))
    }

    val notFoundAccountId = "NOT-GOING-TO-FIND"
    val origPassword = "test password"

    val account = (accountManager.createAccount("test@precog.com", origPassword, new DateTime, AccountPlan.Free) { _ => M.point("testapikey") }).copoint

    def after = {
      defaultActorSystem.shutdown
    }
  }
}
