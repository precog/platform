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

import blueeyes._
import blueeyes.bkka._
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo._

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import akka.util.Timeout
import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import org.joda.time.DateTime
import org.bson.types.ObjectId
import org.I0Itec.zkclient.ZkClient 
import org.I0Itec.zkclient.DataUpdater

import com.weiglewilczek.slf4s.Logging

import org.streum.configrity.Configuration

import scalaz._
import scalaz.syntax.monad._

trait AccountManager[M[+_]] {
  type AccountId = String
  type ApiKey = String

  def newAccountId: M[AccountId]
  
  def newTempPassword(): String = new ObjectId().toString

  def updateAccount(account: Account): M[Boolean]
 
  def newAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan)(f: (AccountId, Path) => M[ApiKey]): M[Account]

  def listAccountIds(apiKey: ApiKey) : M[Set[Account]]
  
  def findAccountById(accountId: AccountId): M[Option[Account]]
  def findAccountByEmail(email: String) : M[Option[Account]]
  def authAccount(email: String, password: String) : M[Option[Account]]
  
  def deleteAccount(accountId: AccountId): M[Option[Account]]

  def close(): M[Unit]
} 


trait ZkAccountManagerSettings {
  def zkAccountIdPath: String
}

trait MongoAccountManagerSettings {
  def accounts: String
  def deletedAccounts: String
  def timeout: Timeout
}


trait ZkMongoAccountManagerComponent extends Logging {
  implicit def asyncContext: ExecutionContext
  implicit lazy val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)

  def accountManager(config: Configuration): AccountManager[Future] = {
    val mongo = RealMongo(config.detach("mongo"))
    
    val zkHosts = config[String]("zookeeper.hosts", "localhost:2181")
    val database = config[String]("mongo.database", "accounts_v1")

    val settings0 = new MongoAccountManagerSettings with ZkAccountManagerSettings {
      val zkAccountIdPath = config[String]("zookeeper.accountId.path")
      val accounts = config[String]("mongo.collection", "accounts")
      val deletedAccounts = config[String]("mongo.deletedCollection", "deleted_accounts")
      val timeout = new Timeout(config[Int]("mongo.timeout", 30000))
    }

    new MongoAccountManager(mongo, mongo.database(database), settings0) with ZkAccountIdSource {
      val settings = settings0
      val zkc = new ZkClient(zkHosts)
    }
  }
}


trait ZkAccountIdSource extends AccountManager[Future] {
  implicit def execContext: ExecutionContext

  def zkc: ZkClient
  def settings: ZkAccountManagerSettings

  def newAccountId: Future[String] = Future {
    if (!zkc.exists(settings.zkAccountIdPath)) {
      zkc.createPersistent(settings.zkAccountIdPath, true)
    }

    val createdPath = zkc.createPersistentSequential(settings.zkAccountIdPath, Array.empty[Byte])
    createdPath.substring(createdPath.length - 10) //last 10 characters are a sequential int
  }
}

abstract class MongoAccountManager(mongo: Mongo, database: Database, settings: MongoAccountManagerSettings)(implicit val execContext: ExecutionContext) extends AccountManager[Future] with Logging {
  import Account._
  private implicit val impTimeout = settings.timeout
  private val randomSource = new java.security.SecureRandom

  def randomSalt() = {
    val saltBytes = new Array[Byte](256)
    randomSource.nextBytes(saltBytes)
    saltBytes.flatMap(byte => Integer.toHexString(0xFF & byte))(collection.breakOut) : String
  }

  private def saltAndHash(password: String, salt: String): String = {
    val md = java.security.MessageDigest.getInstance("SHA-256");
    val dataBytes = (password + salt).getBytes("UTF-8")
    md.update(dataBytes, 0, dataBytes.length)
    val hashBytes = md.digest()

    hashBytes.flatMap(byte => Integer.toHexString(0xFF & byte))(collection.breakOut) : String
  }

  def newAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan)(f: (AccountId, Path) => Future[ApiKey]): Future[Account] = {
    for {
      accountId <- newAccountId
      path = Path(accountId)
      apiKey <- f(accountId, path)
      account <- {
        val salt = randomSalt()
        val account0 = Account(
          accountId, email, 
          saltAndHash(password, salt), salt,
          creationDate,
          apiKey, path, plan)
        
        database(insert(account0.serialize(UnsafeAccountDecomposer).asInstanceOf[JObject]).into(settings.accounts)) map {
          _ => account0
        } 
      }
    } yield account
  }

  private def findOneMatching[A](keyName: String, keyValue: String, collection: String)(implicit extractor: Extractor[A]): Future[Option[A]] = {
    database {
      selectOne().from(collection).where(keyName === keyValue)
    }.map {
      _.map(_.deserialize(extractor))
    }
  }

  private def findAllMatching[A](keyName: String, keyValue: String, collection: String)(implicit extractor: Extractor[A]): Future[Set[A]] = {
    database {
      selectAll.from(collection).where(keyName === keyValue)
    }.map {
      _.map(_.deserialize(extractor)).toSet
    }
  }
  
  private def findAll[A](collection: String)(implicit extract: Extractor[A]): Future[Seq[A]] =
    database { selectAll.from(collection) }.map { _.map(_.deserialize(extract)).toSeq }

  
  def listAccountIds(apiKey: String) =  findAllMatching[Account]("apiKey", apiKey, settings.accounts)
  
  def findAccountById(accountId: String) = findOneMatching[Account]("accountId", accountId, settings.accounts)

  def findAccountByEmail(email: String) = findOneMatching[Account]("email", email, settings.accounts)
  
  def authAccount(email: String, password: String) = {
    for {
      accountOpt <- findAccountByEmail(email)
    } yield {
      accountOpt filter { account =>
        account.passwordHash == saltAndHash(password, account.passwordSalt)
      }
    }
  }

  def updateAccount(account: Account): Future[Boolean] = {
    findAccountById(account.accountId).flatMap {
      case Some(existingAccount) =>
        database {
          val updateObj = account.serialize(UnsafeAccountDecomposer).asInstanceOf[JObject]
          update(settings.accounts).set(updateObj).where("accountId" === account.accountId)
        } map {
          _ => true
        }
          
      case None => 
        Future(false)
    }
  }

  def deleteAccount(accountId: String): Future[Option[Account]] = {
    findAccountById(accountId).flatMap { 
      case ot @ Some(account) =>
        for {
          _ <- database(insert(account.serialize(UnsafeAccountDecomposer).asInstanceOf[JObject]).into(settings.deletedAccounts))
           _ <- database(remove.from(settings.accounts).where("accountId" === accountId))
        } yield { ot }
      case None    => Future(None)
    } 
  }

  def close() = database.disconnect.fallbackTo(Future(())).flatMap{_ => mongo.close}
}




