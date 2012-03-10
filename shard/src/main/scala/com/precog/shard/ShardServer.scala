package com.precog
package shard

import common.security.StaticTokenManager
import ingest.service.NullUsageLogging
import shard.yggdrasil.YggdrasilQueryExecutorComponent

import blueeyes.BlueEyesServer
import blueeyes.util.Clock

import org.streum.configrity.Configuration

trait KafkaShardServer extends BlueEyesServer with ShardService with YggdrasilQueryExecutorComponent {
  
  val clock = Clock.System

  def usageLoggingFactory(config: Configuration) = new NullUsageLogging("")
  def tokenManagerFactory(config: Configuration) = StaticTokenManager 

}
