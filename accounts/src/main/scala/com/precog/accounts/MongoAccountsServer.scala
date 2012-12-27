package com.precog
package accounts

import common.security._

import blueeyes.bkka._
import blueeyes.BlueEyesServer

object MongoAccountServer extends BlueEyesServer with AccountService with ZkMongoAccountManagerComponent with AkkaDefaults {
  val executor = defaultFutureDispatch
  override val M = new FutureMonad(executor)
  val clock = blueeyes.util.Clock.System
}

