package com.precog.heimdall

import blueeyes.persistence.mongo._

import blueeyes.BlueEyesServer

object MongoJobServer extends BlueEyesServer with JobService with MongoJobManagerModule {
  implicit val asyncContext = defaultFutureDispatch

  val clock = blueeyes.util.Clock.System

  lazy val mongo = RealMongo(config.detach("mongo"))

  def close() = mongo.close
}

