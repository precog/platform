package com.precog.shard

import com.precog.ingest.service._

import blueeyes.BlueEyesServer
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo.Mongo
import blueeyes.persistence.mongo.MongoCollection
import blueeyes.persistence.mongo.Database
import blueeyes.util.Clock

import com.precog.analytics.TokenManager

import net.lag.configgy.ConfigMap

trait ShardWebapp extends BlueEyesServer with ShardService {
  def mongoFactory(configMap: ConfigMap): Mongo = {
    new blueeyes.persistence.mongo.MockMongo()
  }

  def usageLogging(config: ConfigMap) = {
    new NullUsageLogging("")
  }

  def tokenManager(database: Database, tokensCollection: MongoCollection, deletedTokensCollection: MongoCollection): TokenManager = 
    new TokenManager(database, tokensCollection, deletedTokensCollection)

  val clock = Clock.System
}
