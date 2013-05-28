package com.precog.heimdall

import com.precog.common.jobs._
import com.precog.common.client._

import akka.dispatch.Future

import blueeyes.bkka._
import blueeyes.persistence.mongo._
import blueeyes.BlueEyesServer

import org.streum.configrity.Configuration
import scalaz._

object MongoJobServer extends BlueEyesServer with JobService with ManagedMongoJobManagerModule with AkkaDefaults {
  implicit val executionContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(defaultFutureDispatch)

  val clock = blueeyes.util.Clock.System

  type JobResource = Mongo

  def close(mongo: Mongo) = mongo.close

  def authService(config0: Configuration): AuthService[Future] = {
    import WebJobManager._

    val config = config0.detach("auth")
    val protocol = config[String]("service.protocol", "http")
    val host = config[String]("service.host", "localhost")
    val port = config[Int]("service.port", 80)
    val path = config[String]("service.path", "/security/v1/")

    WebAuthService(protocol, host, port, path).withM[Future]
  }
}

