package com.precog.common
package accounts

import com.precog.common.Path
import com.precog.util.cache.Cache
import com.precog.common.client._
import com.precog.common.security._
import com.precog.util._

import akka.dispatch.{ ExecutionContext, Future, Promise }

import blueeyes.bkka._
import blueeyes.core.data.DefaultBijections._
import blueeyes.core.data.ByteChunk
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.core.service.engines.HttpClientXLightWeb
import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Extractor._

import org.apache.commons.codec.binary.Base64
import org.joda.time.DateTime
import org.streum.configrity.Configuration
import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.{ NonEmptyList => NEL }
import scalaz.Validation._
import scalaz.syntax.bifunctor._
import scalaz.syntax.monad._
import scalaz.syntax.validation._
import scalaz.syntax.std.option._

object WebAccountFinder extends Logging {
  def apply(config: Configuration)(implicit executor: ExecutionContext): Validation[NonEmptyList[String], AccountFinder[Response]] = {
    val serviceConfig = config.detach("service")
    serviceConfig.get[String]("hardcoded_account") map { accountId =>
      implicit val M = ResponseMonad(new FutureMonad(executor))
      success(new StaticAccountFinder[Response](accountId, serviceConfig[String]("hardcoded_rootKey", ""), serviceConfig.get[String]("hardcoded_rootPath")))
    } getOrElse {
      (serviceConfig.get[String]("protocol").toSuccess(NEL("Configuration property service.protocol is required")) |@|
       serviceConfig.get[String]("host").toSuccess(NEL("Configuration property service.host is required")) |@|
       serviceConfig.get[Int]("port").toSuccess(NEL("Configuration property service.port is required")) |@|
       serviceConfig.get[String]("path").toSuccess(NEL("Configuration property service.path is required")) |@|
       serviceConfig.get[String]("user").toSuccess(NEL("Configuration property service.user is required")) |@|
       serviceConfig.get[String]("password").toSuccess(NEL("Configuration property service.password is required"))) {
        (protocol, host, port, path, user, password) =>
          val cacheSize = serviceConfig[Int]("cache_size", 1000)
          logger.info("Creating new WebAccountFinder with properties %s://%s:%s/%s %s:%s".format(protocol, host, port.toString, path, user, password))
          new WebAccountFinder(protocol, host, port, path, user, password, cacheSize)
      }
    }
  }
}

class WebAccountFinder(protocol: String, host: String, port: Int, path: String, user: String, password: String, cacheSize: Int)(implicit executor: ExecutionContext) extends WebClient(protocol, host, port, path) with AccountFinder[Response] with Logging {
  import scalaz.syntax.monad._
  import EitherT.{ left => leftT, right => rightT, _ }
  import \/.{ left, right }
  import blueeyes.core.data.DefaultBijections._
  import blueeyes.json.serialization.DefaultSerialization._

  implicit val M: Monad[Future] = new FutureMonad(executor)

  private[this] val apiKeyToAccountCache = Cache.simple[APIKey, AccountId](Cache.MaxSize(cacheSize))

  def findAccountByAPIKey(apiKey: APIKey) : Response[Option[AccountId]] = {
    logger.debug("Finding account for API key " + apiKey + " with " + (protocol, host, port, path, user, password).toString)
    apiKeyToAccountCache.get(apiKey).map { id =>
      logger.debug("Cache hit for API key " + apiKey)
      rightT(Promise.successful(Some(id)): Future[Option[AccountId]])
    }.getOrElse {
      invoke { client =>
        logger.info("Cache miss for API key %s, querying accounts service.".format(apiKey))
        eitherT(client.query("apiKey", apiKey).get[JValue]("/accounts/") map {
          case HttpResponse(HttpStatus(OK, _), _, Some(jaccountId), _) =>
            logger.info("Got response for apiKey " + apiKey)
            (((_:Extractor.Error).message) <-: jaccountId.validated[WrappedAccountId] :-> { wid =>
                apiKeyToAccountCache.put(apiKey, wid.accountId)
                Some(wid.accountId)
            }).disjunction

          case HttpResponse(HttpStatus(OK, _), _, None, _) =>
            logger.warn("No account found for apiKey: " + apiKey)
            right(None)

          case res =>
            logger.error("Unexpected response from accounts service for findAccountByAPIKey: " + res)
            left("Unexpected response from accounts service; unable to proceed: " + res)
        } recoverWith {
          case ex =>
            logger.error("findAccountByAPIKey for " + apiKey + "failed.", ex)
            Promise.successful(left("Client error accessing accounts service; unable to proceed: " + ex.getMessage))
        })
      }
    }
  }

  def findAccountDetailsById(accountId: AccountId): Response[Option[AccountDetails]] = {
    logger.debug("Finding accoung for id: " + accountId)
    invoke { client =>
      eitherT(client.get[JValue]("/accounts/" + accountId) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jaccount), _) =>
          logger.info("Got response for AccountId " + accountId)
          (((_:Extractor.Error).message) <-: jaccount.validated[Option[AccountDetails]]).disjunction

        case res =>
          logger.error("Unexpected response from accounts serviceon findAccountDetailsById: " + res)
          left("Unexpected response from accounts service; unable to proceed: " + res)
      } recoverWith {
        case ex =>
          logger.error("findAccountById for " + accountId + "failed.", ex)
          Promise.successful(left("Client error accessing accounts service; unable to proceed: " + ex.getMessage))
      })
    }
  }

  def invoke[A](f: HttpClient[ByteChunk] => A): A = {
    val auth = HttpHeaders.Authorization("Basic "+new String(Base64.encodeBase64((user+":"+password).getBytes("UTF-8")), "UTF-8"))
    withJsonClient { client =>
      f(client.header(auth))
    }
  }
}

