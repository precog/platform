package com.precog.accounts

import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.security.{TZDateTimeDecomposer => _, _}
import com.precog.util.PrecogUnit

import blueeyes._
import blueeyes.bkka._
import blueeyes.json._
import blueeyes.persistence.mongo._
import blueeyes.persistence.mongo.dsl._

import blueeyes.json.serialization.{ Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import akka.util.Timeout
import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import org.joda.time.DateTime
import org.bson.types.ObjectId
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.DataUpdater

import org.slf4j.LoggerFactory

import org.streum.configrity.Configuration

import scalaz._
import scalaz.syntax.monad._

trait ZkAccountManagerSettings {
  def zkAccountIdPath: String
}

trait ZKAccountIdSource extends AccountManager[Future] {
  implicit def M: Monad[Future]
  def zkc: ZkClient
  def settings: ZkAccountManagerSettings

  def newAccountId: Future[String] = M.point {
    if (!zkc.exists(settings.zkAccountIdPath)) {
      zkc.createPersistent(settings.zkAccountIdPath, true)
    }

    val createdPath = zkc.createPersistentSequential(settings.zkAccountIdPath, Array.empty[Byte])
    createdPath.substring(createdPath.length - 10) //last 10 characters are a sequential int
  }
}

trait MongoAccountManagerSettings {
  def accounts: String
  def deletedAccounts: String
  def timeout: Timeout

  def resetTokens: String
  def resetTokenExpirationMinutes: Int
}

abstract class MongoAccountManager(mongo: Mongo, database: Database, settings: MongoAccountManagerSettings)(implicit val M: Monad[Future])
    extends AccountManager[Future] {
  import Account._

  private lazy val mamLogger = LoggerFactory.getLogger("com.precog.accounts.MongoAccountManager")

  private implicit val impTimeout = settings.timeout

  // Ensure indices for account lookup on apiKey, accountId, or email
  database(ensureIndex("apiKey_index").on(".apiKey").in(settings.accounts))
  database(ensureIndex("accountId_index").on(".accountId").in(settings.accounts))
  database(ensureIndex("email_index").on(".email").in(settings.accounts))
  // Ensure reset token lookup by token Id
  database(ensureIndex("reset_token_index").on(".tokenId").in(settings.resetTokens))

  def newAccountId: Future[String]

  def createAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan, parent: Option[AccountId], profile: Option[JValue])(f: AccountId => Future[APIKey]): Future[Account] = {
    for {
      accountId <- newAccountId
      path = Path(accountId)
      apiKey <- f(accountId)
      account <- {
        val salt = randomSalt()
        val account0 = Account(
          accountId, email,
          saltAndHashSHA256(password, salt), salt,
          creationDate,
          apiKey, path, plan,
          parent, Some(creationDate), profile)

        database(insert(account0.serialize.asInstanceOf[JObject]).into(settings.accounts)) map {
          _ => account0
        }
      }
    } yield account
  }

  private def findOneMatching[A](keyName: String, keyValue: String, collection: String)(implicit extractor: Extractor[A]): Future[Option[A]] = {
    database(selectOne().from(collection).where(keyName === keyValue)) map {
      _.map(_.deserialize(extractor))
    }
  }

  private def findAllMatching[A](keyName: String, keyValue: String, collection: String)(implicit extractor: Extractor[A]): Future[Set[A]] = {
    database(selectAll.from(collection).where(keyName === keyValue)) map {
      _.map(_.deserialize(extractor)).toSet
    }
  }

  private def findAll[A](collection: String)(implicit extract: Extractor[A]): Future[Seq[A]] =
    database(selectAll.from(collection)) map {
      _.map(_.deserialize(extract)).toSeq
    }

  def generateResetToken(account: Account): Future[ResetTokenId] =
    generateResetToken(account, (new DateTime).plusMinutes(settings.resetTokenExpirationMinutes))

  def generateResetToken(account: Account, expiration: DateTime): Future[ResetTokenId] = {
    val tokenId = java.util.UUID.randomUUID.toString.replace("-","")

    val token = ResetToken(tokenId, account.accountId, account.email, expiration)

    logger.debug("Saving new reset token " + token)
    database(insert(token.serialize.asInstanceOf[JObject]).into(settings.resetTokens)).map { _ =>
      logger.debug("Save complete on reset token " + token)
      tokenId
    }
  }

  def markResetTokenUsed(tokenId: ResetTokenId): Future[PrecogUnit] = {
    logger.debug("Marking reset token %s as used".format(tokenId))
    database(update(settings.resetTokens).set("usedAt" set (new DateTime).serialize).where("tokenId" === tokenId)).map {
      _ => logger.debug("Reset token %s marked as used".format(tokenId)); PrecogUnit
    }
  }

  def findResetToken(accountId: AccountId, tokenId: ResetTokenId): Future[Option[ResetToken]] =
    findOneMatching[ResetToken]("tokenId", tokenId, settings.resetTokens)

  def findAccountByAPIKey(apiKey: String) = findOneMatching[Account]("apiKey", apiKey, settings.accounts).map(_.map(_.accountId))

  def findAccountById(accountId: String) = findOneMatching[Account]("accountId", accountId, settings.accounts)

  def findAccountByEmail(email: String) = findOneMatching[Account]("email", email, settings.accounts)

  def updateAccount(account: Account): Future[Boolean] = {
    findAccountById(account.accountId).flatMap {
      case Some(existingAccount) =>
        database {
          val updateObj = account.serialize.asInstanceOf[JObject]
          update(settings.accounts).set(updateObj).where("accountId" === account.accountId)
        } map {
          _ => true
        }

      case None =>
        M.point(false)
    }
  }

  def deleteAccount(accountId: String): Future[Option[Account]] = {
    findAccountById(accountId).flatMap {
      case ot @ Some(account) =>
        for {
          _ <- database(insert(account.serialize.asInstanceOf[JObject]).into(settings.deletedAccounts))
          _ <- database(remove.from(settings.accounts).where("accountId" === accountId))
        } yield { ot }
      case None =>
        M.point(None)
    }
  }

  def close() = database.disconnect.fallbackTo(M.point(())).flatMap{_ => mongo.close}
}
