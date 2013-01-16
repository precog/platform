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
    with ShardService 
    with JDBMQueryExecutorComponent 
    with MongoAPIKeyManagerComponent 
    with AccountManagerClientComponent
{
  import WebJobManager._
  
  val clock = Clock.System

  implicit val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)

  def configureShardState(config: Configuration): ShardState = {
    val apiKeyManager = apiKeyManagerFactory(config.detach("security"))
    val accountManager = accountManagerFactory(config.detach("accounts"))
    val jobManager = WebJobManager(config.detach("jobs")).withM[Future]
    val queryExecutorFactory = queryExecutorFactoryFactory(
      config.detach("queryExecutor"), apiKeyManager, accountManager, jobManager)
    ManagedQueryShardState(queryExecutorFactory, apiKeyManager, accountManager, jobManager, clock)
  }
}
