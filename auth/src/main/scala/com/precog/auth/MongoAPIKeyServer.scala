package com.precog
package auth

import common.security._

import blueeyes.BlueEyesServer

object MongoAPIKeyServer extends BlueEyesServer with SecurityService with MongoAPIKeyManagerComponent {
  implicit val asyncContext = defaultFutureDispatch
}

