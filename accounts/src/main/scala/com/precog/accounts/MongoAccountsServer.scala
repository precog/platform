package com.precog.accounts

import com.precog.auth.MongoAPIKeyManager
import com.precog.common.client._
import com.precog.common.security._
import com.precog.common.security.service._

import blueeyes.bkka._
import blueeyes.BlueEyesServer
import blueeyes.persistence.mongo._

import akka.dispatch.Future
import akka.util.Timeout

import org.I0Itec.zkclient.ZkClient
import org.streum.configrity.Configuration

import scalaz._

object MongoAccountServer extends BlueEyesServer with AccountService with AkkaDefaults {
  val executionContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  val clock = blueeyes.util.Clock.System

  def AccountManager(config: Configuration): (AccountManager[Future], Stoppable) = {
    val mongo = RealMongo(config.detach("mongo"))

    val zkHosts = config[String]("zookeeper.hosts", "localhost:2181")
    val database = config[String]("mongo.database", "accounts_v1")

    val settings0 = new MongoAccountManagerSettings with ZkAccountManagerSettings {
      val zkAccountIdPath = config[String]("zookeeper.accountId.path")
      val accounts = config[String]("mongo.collection", "accounts")
      val deletedAccounts = config[String]("mongo.deletedCollection", "deleted_accounts")
      val timeout = new Timeout(config[Int]("mongo.timeout", 30000))
    }

    val accountManager = new MongoAccountManager(mongo, mongo.database(database), settings0) with ZKAccountIdSource {
      val zkc = new ZkClient(zkHosts)
      val settings = settings0
    }

    (accountManager, Stoppable.fromFuture(accountManager.close()))
  }

  def APIKeyFinder(config: Configuration) = MongoAPIKeyManager(config)

//  def APIKeyFinder(config: Configuration) = WebAPIKeyFinder(config).map(_.withM[Future]) valueOr { errs =>
//    sys.error("Unable to build new WebAPIKeyFinder: " + errs.list.mkString("\n", "\n", ""))
//  }
}
