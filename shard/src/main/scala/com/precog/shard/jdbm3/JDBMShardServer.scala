package com.precog.shard
package jdbm3

import com.precog.common.security._
import com.precog.ingest.service.NullUsageLogging

import blueeyes.BlueEyesServer
import blueeyes.util.Clock

import org.streum.configrity.Configuration

object JDBMShardServer extends BlueEyesServer with ShardService with JDBMQueryExecutorComponent with MongoAPIKeyManagerComponent {
  
  val clock = Clock.System

  def usageLoggingFactory(config: Configuration) = new NullUsageLogging("")

  val asyncContext = defaultFutureDispatch
}
