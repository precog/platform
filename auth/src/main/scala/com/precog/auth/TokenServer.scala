package com.precog
package auth

import common.security._

import blueeyes.BlueEyesServer

object MongoTokenServer extends BlueEyesServer with TokenService with MongoTokenManagerComponent

