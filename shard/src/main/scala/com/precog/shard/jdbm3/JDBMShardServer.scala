package com.precog.shard
package jdbm3

import com.precog.auth.MongoAPIKeyManagerComponent
import com.precog.accounts.AccountManagerClientComponent
import com.precog.common.security._

import akka.dispatch.Future

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.util.Clock

import scalaz._

object JDBMShardServer extends BlueEyesServer 
    with ShardService 
    with JDBMQueryExecutorComponent 
    with MongoAPIKeyManagerComponent 
    with AccountManagerClientComponent
{
  
  val clock = Clock.System

  val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)
}
