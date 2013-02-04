package com.precog.common
package security
package service

import client._
import accounts.AccountId

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
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }

import org.joda.time.DateTime
import org.joda.time.format.ISODateTimeFormat
import org.streum.configrity.Configuration

import scalaz._
import scalaz.EitherT.eitherT
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.std.set._

object WebAPIKeyFinder {
  def apply(config: Configuration)(implicit executor: ExecutionContext): APIKeyFinder[Future] = {
    new WebAPIKeyFinder(
      config[String]("protocol", "http"),
      config[String]("host", "localhost"),
      config[Int]("port", 80),
      config[String]("path", "/security/v1/"),
      config[String]("rootKey")
    )
  }
}

class WebAPIKeyFinder(protocol: String, host: String, port: Int, path: String, rootAPIKey: APIKey)(implicit executor: ExecutionContext) 
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
    withJsonClient { client =>
      client.query("apiKey", fromRoot).get[JValue]("apikeys/") map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jvalue), _) =>
          jvalue.validated[Set[v1.APIKeyDetails]] getOrElse Set.empty
        case res =>
          logger.warn("Unexpected response from auth service for apiKey " + fromRoot + ":\n" + res)
          Set.empty
      }
    }
  }

  private val fmt = ISODateTimeFormat.dateTime()

  private def findPermissions(apiKey: APIKey, path: Path, at: Option[DateTime]): Future[Set[Permission]] = {
    withJsonClient { client0 =>
      val client = at map (fmt.print(_)) map (client0.query("at", _)) getOrElse client0
      client.query("apiKey", apiKey).get[JValue]("/permissions/fs/" + path) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jvalue), _) =>
          jvalue.validated[Set[Permission]] getOrElse Set.empty
        case res =>
          logger.warn("Unexpected response from auth service for apiKey " + apiKey + ":\n" + res)
          Set.empty
      }
    }
  }

  def hasCapability(apiKey: APIKey, perms: Set[Permission], at: Option[DateTime]): Future[Boolean] = {

    // We group permissions by path, then find the permissions for each path individually.
    // This means a call to this will do 1 HTTP request per path, which isn't efficient.

    val results: Set[Future[Boolean]] = perms.groupBy(_.path).map({ case (path, requiredPathPerms) =>
      findPermissions(apiKey, path, at) map { actualPathPerms =>
        requiredPathPerms forall { perm => actualPathPerms exists (_ implies perm) }
      }
    })(collection.breakOut)
    results.sequence.map(_.foldLeft(true)(_ && _))
  }

  def newAPIKey(accountId: AccountId, path: Path, keyName: Option[String] = None, keyDesc: Option[String] = None): Future[v1.APIKeyDetails] = {
    val keyRequest = v1.NewAPIKeyRequest.newAccount(accountId, path, None, None)

    withJsonClient { client => 
      client.query("apiKey", rootAPIKey).post[JValue]("apiKeys/")(keyRequest.serialize) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(wrappedKey), _) =>
          wrappedKey.validated[v1.APIKeyDetails] valueOr { error =>
            logger.error("Unable to deserialize response from auth service: " + error.message)
            throw HttpException(BadGateway, "Unexpected response to API key creation request: " + error.message)
          }

        case HttpResponse(HttpStatus(failure: HttpFailure, reason), _, content, _) => 
          logger.error("Fatal error attempting to create api key: " + failure + ": " + content)
          throw HttpException(failure, reason)

        case x => 
          logger.error("Unexpected response from api provisioning service: " + x)
          throw HttpException(BadGateway, "Unexpected response from the api provisioning service: " + x)
      }
    }
  }

  def addGrant(authKey: APIKey, accountKey: APIKey, grantId: GrantId): Future[Boolean] = {
    val requestBody = jobject(JField("grantId", JString(grantId)))

    withJsonClient { client =>
      client.query("apiKey", authKey).post[JValue]("apikeys/" + accountKey + "/grants/")(requestBody) map {
        case HttpResponse(HttpStatus(Created, _), _, None, _) => 
          true
        
        case _ =>
          false
      }
    }
  }
}



// vim: set ts=4 sw=4 et:
