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

object JDBMShardServer extends BlueEyesServer with AsyncShardService with AkkaDefaults {
  import WebJobManager._
  val clock = Clock.System

  val executionContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  def APIKeyFinder(config: Configuration): APIKeyFinder[Future] = WebAPIKeyFinder(config)
  def AccountFinder(config: Configuration): AccountFinder[Future] = WebAccountFinder(config)
  def jobManagerFactory(config: Configuration): JobManager[Future] = WebJobManager(config).withM[Future]
  def QueryExecutor(config: Configuration, accessControl: AccessControl[Future], accountFinder: AccountFinder[Future], jobManager: JobManager[Future]) = 
    JDBMQueryExecutor(config, accessControl, accountFinder, jobManager)
}
