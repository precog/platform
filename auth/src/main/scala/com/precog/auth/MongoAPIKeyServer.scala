package com.precog
package auth

import common.security._

import akka.dispatch.{ ExecutionContext, Future }

import blueeyes.bkka._
import blueeyes.BlueEyesServer

import scalaz._

object MongoAPIKeyServer extends BlueEyesServer with SecurityService with MongoAPIKeyManagerComponent {
  implicit val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)
}
