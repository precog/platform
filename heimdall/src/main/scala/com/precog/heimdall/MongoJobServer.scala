package com.precog.heimdall

import blueeyes.persistence.mongo._

import blueeyes.BlueEyesServer

object MongoJobServer extends BlueEyesServer with JobService with ManagedMongoJobManagerModule {
  implicit val asyncContext = defaultFutureDispatch

  val clock = blueeyes.util.Clock.System

  type Resource = Mongo

  def close(mongo: Mongo) = mongo.close
}

