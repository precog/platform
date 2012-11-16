package com.precog
package accounts

import com.precog.common.Path

import blueeyes._
import blueeyes.bkka._
import blueeyes.json._
import blueeyes.persistence.mongo._

import com.google.common.base.Charsets
import com.google.common.hash.Hashing

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

trait AccountManager[M[+_]] {
  type AccountId = String
  type ApiKey = String

  def newAccountId: M[AccountId]
  
  def newTempPassword(): String = new ObjectId().toString

  def updateAccount(account: Account): M[Boolean]
  def updateAccountPassword(account: Account, newPassword: String): M[Boolean]
 
  def newAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan, parentId: Option[AccountId] = None)(f: (AccountId, Path) => M[ApiKey]): M[Account]

  def listAccountIds(apiKey: ApiKey) : M[Set[Account]]
  
  def findAccountById(accountId: AccountId): M[Option[Account]]
  def findAccountByEmail(email: String) : M[Option[Account]]
  def authAccount(email: String, password: String) : M[Option[Account]]
  def hasAncestor(child: Account, ancestor: Account) : M[Boolean]

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

abstract class MongoAccountManager(mongo: Mongo, database: Database, settings: MongoAccountManagerSettings)(implicit val execContext: ExecutionContext) extends AccountManager[Future] {
  import Account._

  private lazy val mamLogger = LoggerFactory.getLogger("com.precog.accounts.MongoAccountManager")

  private implicit val impTimeout = settings.timeout
  private val randomSource = new java.security.SecureRandom

  def randomSalt() = {
    val saltBytes = new Array[Byte](256)
    randomSource.nextBytes(saltBytes)
    saltBytes.flatMap(byte => Integer.toHexString(0xFF & byte))(collection.breakOut) : String
  }

  private def saltAndHash(password: String, salt: String): String = {
    Hashing.sha1().hashString(password + salt, Charsets.UTF_8).toString
  }

  def newAccount(email: String, password: String, creationDate: DateTime, plan: AccountPlan, parent: Option[AccountId] = None)(f: (AccountId, Path) => Future[ApiKey]): Future[Account] = {
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

  
  def listAccountIds(apiKey: String) = findAllMatching[Account]("apiKey", apiKey, settings.accounts)
  
  def findAccountById(accountId: String) = findOneMatching[Account]("accountId", accountId, settings.accounts)

  def findAccountByEmail(email: String) = findOneMatching[Account]("email", email, settings.accounts)

  def hasAncestor(child: Account, ancestor: Account) = {
    if (child == ancestor) {
      Future(true)
    } else {
      child.parentId map { id =>
        findAccountById(id) flatMap {
          case None => Future(false)
          case Some(parent) => hasAncestor(parent, ancestor)
        }
      } getOrElse {
        Future(false)
      }
    }
  }
  
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

  def updateAccountPassword(account: Account, newPassword: String): Future[Boolean] = {
    val salt = randomSalt()
    updateAccount(account.copy(passwordHash = saltAndHash(newPassword, salt), passwordSalt = salt))
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




