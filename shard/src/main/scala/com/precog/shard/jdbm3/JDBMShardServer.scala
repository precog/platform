package com.precog.shard
package jdbm3

import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.accounts._
import com.precog.common.jobs._
import com.precog.common.client.BaseClient._

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.util.Clock

import akka.dispatch.Future

import org.streum.configrity.Configuration

import scalaz._

object JDBMShardServer extends BlueEyesServer with ShardService with AkkaDefaults {
  import WebJobManager._
  val clock = Clock.System

  val executionContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  def configureShardState(config: Configuration): ShardState = {
    val apiKeyFinder = WebAPIKeyFinder(config.detach("security"))
    val accountFinder = WebAccountFinder(config.detach("accounts"))
    val jobManager = WebJobManager(config.detach("jobs")).withM[Future]
    val queryExecutorFactory = JDBMQueryExecutorFactory(config.detach("queryExecutor"), apiKeyFinder, accountFinder, jobManager)

    ManagedQueryShardState(queryExecutorFactory, apiKeyFinder, jobManager, clock)
  }
}
