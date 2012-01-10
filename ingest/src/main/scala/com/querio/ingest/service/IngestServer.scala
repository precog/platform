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
package com.querio.ingest.service
import  service._
import  external._

import blueeyes.BlueEyesServer
import blueeyes.concurrent.Future
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo.Mongo
import blueeyes.persistence.mongo.MongoCollection
import blueeyes.persistence.mongo.Database
import blueeyes.util.Clock

import scalaz.NonEmptyList

import com.reportgrid.analytics.TokenManager
import com.reportgrid.api.Server
import com.reportgrid.api.blueeyes.ReportGrid

import java.util.Properties
import java.util.Date
import net.lag.configgy.ConfigMap

import com.querio.ingest.api._
import com.querio.ingest.util.QuerioZookeeper

object IngestServer extends BlueEyesServer with IngestService {

  def mongoFactory(configMap: ConfigMap): Mongo = {
    new blueeyes.persistence.mongo.RealMongo(configMap)
  }

  def storageReporting(config: ConfigMap) = {
    val token = config.getString("token") getOrElse {
      throw new IllegalStateException("storageReporting.tokenId must be specified in application config file. Service cannot start.")
    }

    val environment = config.getString("environment", "dev") match {
      case "production" => Server.Production
      case "dev"        => Server.Dev
      case _            => Server.Local
    }

    //new ReportGridStorageReporting(token, ReportGrid(token, environment))
    new NullStorageReporting(token)
  }

  def auditClient(config: ConfigMap) = {
    NoopTrackingClient
    //val auditToken = config.configMap("audit.token")
    //val environment = config.getString("environment", "production") match {
    //  case "production" => Server.Production
    //  case _            => Server.Local
    //}

    //ReportGrid(auditToken, environment)
  }

  def tokenManager(database: Database, tokensCollection: MongoCollection, deletedTokensCollection: MongoCollection): TokenManager = 
    new TokenManager(database, tokensCollection, deletedTokensCollection)

  def eventStoreFactory(eventConfig: ConfigMap): EventStore = {
    val topicId = eventConfig.getString("topicId").getOrElse(sys.error("Invalid configuration eventStore.topicId required"))
    val zookeeperHosts = eventConfig.getString("zookeeperHosts").getOrElse(sys.error("Invalid configuration eventStore.zookeeperHosts required"))
    
    val props = new Properties()
    props.put("zk.connect", zookeeperHosts)
    props.put("serializer.class", "com.querio.ingest.api.IngestMessageCodec")
    
    val defaultAddresses = NonEmptyList(MailboxAddress(0))

    val routeTable = new ConstantRouteTable(defaultAddresses)
    val messaging = new SimpleKafkaMessaging(topicId, props)

    val qz = QuerioZookeeper.testQuerioZookeeper(zookeeperHosts)
    val producerId = qz.acquireProducerId
    qz.close

    new EventStore(new EventRouter(routeTable, messaging), producerId)
  }

  val clock = Clock.System
}

// vim: set ts=4 sw=4 et:
