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
package com.precog.accounts

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
  val executor = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(executor)

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

  def APIKeyFinder(config: Configuration) = WebAPIKeyFinder(config)
}

