package com.precog.ingest

import service._

import blueeyes.BlueEyesServer
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo.Mongo
import blueeyes.persistence.mongo.MongoCollection
import blueeyes.persistence.mongo.Database
import blueeyes.util.Clock

import com.precog.analytics.TokenManager

import org.streum.configrity.Configuration

trait IngestServer extends BlueEyesServer with IngestService {
  def mongoFactory(config: Configuration): Mongo = {
    new blueeyes.persistence.mongo.MockMongo()
  }

  def usageLogging(config: Configuration) = {
    new NullUsageLogging("")
  }

  def tokenManager(database: Database, tokensCollection: MongoCollection, deletedTokensCollection: MongoCollection): TokenManager = 
    new TokenManager(database, tokensCollection, deletedTokensCollection)

  val clock = Clock.System
}
