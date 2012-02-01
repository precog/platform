package com.precog.ingest.service
import  service._

import blueeyes.BlueEyesServer
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo.Mongo
import blueeyes.persistence.mongo.MongoCollection
import blueeyes.persistence.mongo.Database
import blueeyes.util.Clock

import scalaz.NonEmptyList

import com.precog.analytics.TokenManager

import java.util.Properties
import java.util.Date
import net.lag.configgy.ConfigMap

import com.precog.ingest.api._
import com.precog.ingest.util.QuerioZookeeper

object IngestServer extends BlueEyesServer with IngestService {

  def mongoFactory(configMap: ConfigMap): Mongo = {
    blueeyes.persistence.mongo.RealMongo(configMap)
  }

  def storageReporting(config: ConfigMap) = {
    val token = config.getString("token") getOrElse {
      throw new IllegalStateException("storageReporting.tokenId must be specified in application config file. Service cannot start.")
    }

//    val environment = config.getString("environment", "dev") match {
//      case "production" => Server.Production
//      case "dev"        => Server.Dev
//      case _            => Server.Local
//    }

    //new ReportGridStorageReporting(token, ReportGrid(token, environment))
    new NullStorageReporting(token)
  }

  //def auditClient(config: ConfigMap) = {
    //NoopTrackingClient
    //val auditToken = config.configMap("audit.token")
    //val environment = config.getString("environment", "production") match {
    //  case "production" => Server.Production
    //  case _            => Server.Local
    //}

    //ReportGrid(auditToken, environment)
  //}

  def tokenManager(database: Database, tokensCollection: MongoCollection, deletedTokensCollection: MongoCollection): TokenManager = 
    new TokenManager(database, tokensCollection, deletedTokensCollection)

  def eventStoreFactory(eventConfig: ConfigMap): EventStore = {
    val topicId = eventConfig.getString("topicId").getOrElse(sys.error("Invalid configuration eventStore.topicId required"))
    val zookeeperHosts = eventConfig.getString("zookeeperHosts").getOrElse(sys.error("Invalid configuration eventStore.zookeeperHosts required"))
    
    val props = new Properties()
    props.put("zk.connect", zookeeperHosts)
    props.put("serializer.class", "com.precog.ingest.api.IngestMessageCodec")
    
    val defaultAddresses = NonEmptyList(MailboxAddress(0))

    val routeTable = new ConstantRouteTable(defaultAddresses)
    val messaging = new SimpleKafkaMessaging(topicId, props)

    val qz = QuerioZookeeper.testQuerioZookeeper(zookeeperHosts)
    qz.setup
    val producerId = qz.acquireProducerId
    qz.close

    new EventStore(new EventRouter(routeTable, messaging), producerId)
  }

  val clock = Clock.System
}

// vim: set ts=4 sw=4 et:
