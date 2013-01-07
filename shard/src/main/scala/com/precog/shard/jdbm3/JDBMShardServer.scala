package com.precog.shard
package jdbm3

import com.precog.auth.MongoAPIKeyManagerComponent
import com.precog.accounts.AccountManagerClientComponent
import com.precog.common.security._
import com.precog.common.jobs._

import akka.dispatch.Future

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.util.Clock

import org.streum.configrity.Configuration

import scalaz._

object JDBMShardServer extends BlueEyesServer 
    with AsyncShardService 
    with JDBMQueryExecutorComponent 
    with MongoAPIKeyManagerComponent 
    with AccountManagerClientComponent
{
  import WebJobManager._
  
  val clock = Clock.System

  implicit val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)

  def jobManagerFactory(config: Configuration): JobManager[Future] = WebJobManager(config).withM[Future]
}
