package com.precog
package auth

import common.security._

import akka.dispatch.{ ExecutionContext, Future }

import blueeyes.bkka._
import blueeyes.BlueEyesServer

import org.streum.configrity.Configuration

import scalaz._

object MongoAPIKeyServer extends BlueEyesServer with SecurityService with AkkaDefaults {
  implicit val executionContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(executionContext)
  def APIKeyManager(config: Configuration): (APIKeyManager[Future], Stoppable) = MongoAPIKeyManager(config)
}
