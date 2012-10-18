package com.precog
package accounts

import common.security._

import blueeyes.BlueEyesServer

object MongoAccountServer extends BlueEyesServer with AccountService with ZkMongoAccountManagerComponent {
  implicit val asyncContext = defaultFutureDispatch
  val clock = blueeyes.util.Clock.System
}

