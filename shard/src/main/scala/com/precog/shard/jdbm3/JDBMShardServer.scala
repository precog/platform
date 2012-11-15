package com.precog.shard
package jdbm3

import com.precog.common.security._

import akka.dispatch.Future

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.util.Clock

import scalaz._

object JDBMShardServer extends BlueEyesServer with ShardService with JDBMQueryExecutorComponent with MongoAPIKeyManagerComponent {
  
  val clock = Clock.System

  val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)
}
