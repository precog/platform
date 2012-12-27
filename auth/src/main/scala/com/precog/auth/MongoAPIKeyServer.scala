package com.precog
package auth

import common.security._

import akka.dispatch.{ ExecutionContext, Future }

import blueeyes.bkka._
import blueeyes.BlueEyesServer

import org.streum.configrity.Configuration

import scalaz._

object MongoAPIKeyServer extends BlueEyesServer with SecurityService with AkkaDefaults {
  implicit val executor = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(executor)
  def APIKeyManager(config: Configuration): APIKeyManager[Future] = MongoAPIKeyManager(config)
}
