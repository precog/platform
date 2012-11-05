package com.precog.shard
package mongo

import com.precog.common.security._
import com.precog.ingest.service.NullUsageLogging

import blueeyes.BlueEyesServer
import blueeyes.util.Clock

import org.streum.configrity.Configuration

object MongoShardServer extends BlueEyesServer with ShardService with MongoQueryExecutorComponent with StaticAPIKeyManagerComponent {
  
  val clock = Clock.System

  def usageLoggingFactory(config: Configuration) = new NullUsageLogging("")

  val asyncContext = defaultFutureDispatch
}
