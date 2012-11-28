package com.precog
package accounts

import com.precog.common.Path
import com.precog.common.security._

import blueeyes._
import blueeyes.bkka._
import blueeyes.json._
import blueeyes.persistence.mongo._

import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
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

trait MongoAccountManagerSettings {
  def accounts: String
  def deletedAccounts: String
  def timeout: Timeout
}


trait ZkMongoAccountManagerComponent {
  private lazy val zkmLogger = LoggerFactory.getLogger("com.precog.accounts.ZkMongoAccountManagerComponent")

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

    new MongoAccountManager(mongo, mongo.database(database), settings0) {
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

abstract class MongoAccountManager(mongo: Mongo, database: Database, settings: MongoAccountManagerSettings)(implicit val execContext: ExecutionContext)
  extends AccountManager[Future] with ZkAccountIdSource {
  import Account._

  implicit val M = AkkaTypeClasses.futureApplicative(execContext)
  
  private lazy val mamLogger = LoggerFactory.getLogger("com.precog.accounts.MongoAccountManager")

  private implicit val impTimeout = settings.timeout
  
  def newAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan, parent: Option[AccountID] = None)(f: (AccountID, Path) => Future[APIKey]): Future[Account] = {
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
          apiKey, path, plan,
          parent)
        
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

  
  def listAccountIds(apiKey: String) = findAllMatching[Account]("apiKey", apiKey, settings.accounts).map(_.map(_.accountId))
  
  def mapAccountIds(apiKeys: Set[APIKey]) : Future[Map[APIKey, Set[AccountID]]] =
    apiKeys.foldLeft(Future(Map.empty[APIKey, Set[AccountID]])) {
      case (fmap, key) => fmap.flatMap { m => listAccountIds(key).map { ids => m + (key -> ids) } }
    }
  
  def findAccountById(accountId: String) = findOneMatching[Account]("accountId", accountId, settings.accounts)

  def findAccountByEmail(email: String) = findOneMatching[Account]("email", email, settings.accounts)
  
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




