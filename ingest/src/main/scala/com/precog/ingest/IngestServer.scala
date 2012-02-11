package com.precog.ingest

import service._

import blueeyes.BlueEyesServer
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo.Mongo
import blueeyes.persistence.mongo.MongoCollection
import blueeyes.persistence.mongo.Database
import blueeyes.util.Clock

import com.precog.analytics.TokenManager

import net.lag.configgy.ConfigMap

trait IngestServer extends BlueEyesServer with IngestService {
  def mongoFactory(configMap: ConfigMap): Mongo = {
    new blueeyes.persistence.mongo.MockMongo()
    //blueeyes.persistence.mongo.RealMongo(configMap)
  }

  def usageLogging(config: ConfigMap) = {
    val token = config.getString("token") getOrElse {
      throw new IllegalStateException("usageLogging.tokenId must be specified in application config file. Service cannot start.")
    }

    new NullUsageLogging(token)
  }

  def tokenManager(database: Database, tokensCollection: MongoCollection, deletedTokensCollection: MongoCollection): TokenManager = 
    new TokenManager(database, tokensCollection, deletedTokensCollection)

  val clock = Clock.System
}
