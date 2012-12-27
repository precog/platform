package com.precog.shard
package jdbm3

import com.precog.common.security._
import com.precog.common.accounts._

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.util.Clock

import akka.dispatch.Future

import org.streum.configrity.Configuration

import scalaz._

object JDBMShardServer extends BlueEyesServer with ShardService with AkkaDefaults {
  val clock = Clock.System

  val executionContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  def APIKeyFinder(config: Configuration): APIKeyFinder[Future] = WebAPIKeyFinder(config)
  def AccountFinder(config: Configuration): AccountFinder[Future] = WebAccountFinder(config)
  def QueryExecutor(config: Configuration, accessControl: AccessControl[Future], accountFinder: AccountFinder[Future]) = JDBMQueryExecutor(config, accessControl, accountFinder)
}
