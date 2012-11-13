package com.precog
package shard

import common.security._
import shard.yggdrasil.YggdrasilQueryExecutorComponent

import akka.dispatch.{ ExecutionContext, Future }

import blueeyes.bkka._

import blueeyes.BlueEyesServer
import blueeyes.util.Clock

import org.streum.configrity.Configuration

import scalaz._

object KafkaShardServer extends BlueEyesServer with ShardService with YggdrasilQueryExecutorComponent with MongoAPIKeyManagerComponent {
  
  val clock = Clock.System

  val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)
}
