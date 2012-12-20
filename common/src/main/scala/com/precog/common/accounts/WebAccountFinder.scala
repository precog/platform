package com.precog.common
package accounts

import com.precog.common.Path
import com.precog.common.cache.Cache
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
import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeDecomposer => _, DateTimeExtractor => _, _ }
import blueeyes.json.serialization.Extractor._

import org.apache.commons.codec.binary.Base64
import org.joda.time.DateTime
import org.streum.configrity.Configuration
import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.monad._

trait WebAccountFinderComponent {
  implicit def asyncContext: ExecutionContext
  implicit val M: Monad[Future]

  def AccountFinder(config: Configuration): AccountFinder[Future] = {
    config.get[String]("service.hardcoded_account").map { accountId =>
      new ConstantAccountFinder(accountId)
    }.getOrElse {
      val protocol = config[String]("service.protocol")
      val host = config[String]("service.host")
      val port = config[Int]("service.port")
      val path = config[String]("service.path")
      val user = config[String]("service.user")
      val password = config[String]("service.password")
      val cacheSize = config[Int]("service.cache_size", 1000)
      
      val settings = WebAccountFinderSettings(protocol, host, port, path, user, password, cacheSize)
      new WebAccountFinder(settings)
    }
  }
}

class ConstantAccountFinder(accountId: AccountId) extends AccountFinder[Future] with AkkaDefaults with Logging {
  val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(asyncContext)

  def findAccountByAPIKey(apiKey: APIKey) : Future[Option[AccountId]] = Promise.successful(Some(accountId))
  def findAccountById(accountId: AccountId): Future[Option[Account]] = Promise.successful(None)
}

case class WebAccountFinderSettings(protocol: String, host: String, port: Int, path: String, user: String, password: String, cacheSize: Int)

class WebAccountFinder(settings: WebAccountFinderSettings) extends AccountFinder[Future] with AkkaDefaults with Logging {
  import settings._

  private[this] val apiKeyToAccountCache = Cache.simple[APIKey, AccountId](Cache.MaxSize(cacheSize))

  val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)
  
  def findAccountByAPIKey(apiKey: APIKey) : Future[Option[AccountId]] = {
    apiKeyToAccountCache.get(apiKey).map(id => Promise.successful(Some(id))).getOrElse {
      invoke { client =>
        client.query("apiKey", apiKey).contentType(application/MimeTypes.json).get[JValue]("") map {
          case HttpResponse(HttpStatus(OK, _), _, Some(jaccounts), _) =>
            jaccounts.validated[Set[WrappedAccountId]] match {
              case Success(accountIds) => 
                if (accountIds.size > 1) {
                  // FIXME: The underlying apparatus should now be modified such that
                  // this case can be interpreted as a fatal error
                  logger.error("Found more than one account for API key: " + apiKey + 
                               "; proceeding with random account from those returned.")
                } 

                accountIds.headOption.map(_.accountId) tap {
                  _ foreach { apiKeyToAccountCache.put(apiKey, _) }
                }
              
              case Failure(err) =>
                logger.error("Unexpected response to account list request: " + err)
                throw HttpException(BadGateway, "Unexpected response to account list request: " + err)
            }

          case HttpResponse(HttpStatus(failure: HttpFailure, reason), _, content, _) =>
            logger.error("Fatal error attempting to list accounts: " + failure + ": " + content)
            throw HttpException(failure, reason)

          case other =>
            logger.error("Unexpected response from accounts service: " + other)
            throw HttpException(BadGateway, "Unexpected response from accounts service: " + other)
        }
      }
    }
  }

  def findAccountById(accountId: AccountId): Future[Option[Account]] = {
    import Account.Serialization._
    invoke { client =>
      client.contentType(application/MimeTypes.json).get[JValue](accountId) map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jaccount), _) =>
         jaccount.validated[Option[Account]] match {
           case Success(accounts) => accounts
           case Failure(err) =>
            logger.error("Unexpected response to find account request: " + err)
            throw HttpException(BadGateway, "Unexpected response to find account request: " + err)
         }

        case HttpResponse(HttpStatus(failure: HttpFailure, reason), _, content, _) => 
          logger.error("Fatal error attempting to find account: " + failure + ": " + content)
          throw HttpException(failure, reason)

        case other => 
          logger.error("Unexpected response from accounts service: " + other)
          throw HttpException(BadGateway, "Unexpected response from accounts service: " + other)
      }
    }
  }

  def invoke[A](f: HttpClient[ByteChunk] => A): A = {
    val client = new HttpClientXLightWeb 
    val auth = HttpHeaders.Authorization("Basic "+new String(Base64.encodeBase64((user+":"+password).getBytes("UTF-8")), "UTF-8"))
    f(client.protocol(protocol).host(host).port(port).path(path).header(auth))
  }
}
