package com.precog.common
package security
package service

import client._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import blueeyes.bkka._
import blueeyes.core.data._
import blueeyes.core.data.DefaultBijections._
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.http.HttpStatusCodes.{ Response => _, _ }
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.SerializationImplicits._

import org.joda.time.DateTime
import org.streum.configrity.Configuration

import scalaz._
import scalaz.EitherT.eitherT
import scalaz.syntax.monad._

object WebAPIKeyFinder {
  def apply(config: Configuration)(implicit M: Monad[Future]): APIKeyFinder[Future] = {
    sys.error("todo")
  }
}

class WebAPIKeyFinder(protocol: String, host: String, port: Int, path: String)(implicit executor: ExecutionContext) 
    extends WebClient(protocol, host, port, path) with APIKeyFinder[Future] {

  implicit val M = new FutureMonad(executor)

  def findAPIKey(apiKey: APIKey): Future[Option[v1.APIKeyDetails]] = {
    withJsonClient { client =>
      client.query("apiKey", apiKey).get[JValue]("apikeys/" + apiKey) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jvalue), _) => 
          jvalue.validated[v1.APIKeyDetails].toOption

        case res => 
          logger.warn("Unexpected response from auth service for apiKey " + apiKey + ":\n" + res)
          None
      }
    }
  }

  def findAllAPIKeys(fromRoot: APIKey): Future[Set[v1.APIKeyDetails]] = {
    sys.error("todo")
  }

  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]): Future[Boolean] = {
    sys.error("todo")
  }
}



// vim: set ts=4 sw=4 et:
