package com.precog.standalone

import akka.dispatch.Future

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.util.Clock

import scalaz.Monad

import com.precog.common.security._
import com.precog.common.accounts._
import com.precog.common.jobs._
import com.precog.ingest._
import com.precog.ingest.kafka._

import org.streum.configrity.Configuration

object StandaloneIngestServer
    extends BlueEyesServer
    with EventService
    with AkkaDefaults {
  val clock = Clock.System

  def APIKeyFinder(config: Configuration): APIKeyFinder[Future]
    = new StaticAPIKeyFinder[Future](config[String]("security.masterAccount.apiKey"))
  def AccountFinder(config: Configuration): AccountFinder[Future] = sys.error("todo")
  def EventStore(config: Configuration): EventStore = sys.error("todo")
  def JobManager(config: Configuration): JobManager[Future] = sys.error("todo")

  val executionContext = defaultFutureDispatch
  def asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(asyncContext)
}
