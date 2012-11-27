package com.precog.heimdall

import akka.dispatch.Future

import blueeyes.bkka.AkkaTypeClasses._
import blueeyes.persistence.mongo._
import blueeyes.BlueEyesServer

import org.streum.configrity.Configuration

object MongoJobServer extends BlueEyesServer with JobService with ManagedMongoJobManagerModule {
  implicit val executionContext = defaultFutureDispatch

  val clock = blueeyes.util.Clock.System

  type Resource = Mongo

  def close(mongo: Mongo) = mongo.close

  def authService(config0: Configuration): AuthService[Future] = {
    import WebJobManager._

    val config = config0.detach("auth")
    val protocol = config[String]("protocol", "http")
    val host = config[String]("host", "localhost")
    val port = config[Int]("port", 30062)
    val path = config[String]("path", "auth")

    WebAuthService(protocol, host, port, path).withM[Future]
  }
}

