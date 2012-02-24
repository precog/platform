package com.precog.ingest

import service._

import blueeyes.BlueEyesServer
import blueeyes.json.JsonAST._
import blueeyes.persistence.mongo.Mongo
import blueeyes.persistence.mongo.MongoCollection
import blueeyes.persistence.mongo.Database
import blueeyes.util.Clock

import com.precog.common.security.StaticTokenManager

import org.streum.configrity.Configuration

trait IngestServer extends BlueEyesServer with IngestService {

  def usageLoggingFactory(config: Configuration) = new NullUsageLogging("")

  def tokenManagerFactory(config: Configuration) = StaticTokenManager 

  val clock = Clock.System
}
