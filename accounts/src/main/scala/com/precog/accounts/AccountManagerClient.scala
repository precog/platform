package com.precog
package accounts

import com.precog.common.Path
import com.precog.common.security._

import akka.dispatch.{ ExecutionContext, Future }

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

import org.joda.time.DateTime
import org.streum.configrity.Configuration
import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.monad._

case class AccountManagerClientSettings(protocol: String, host: String, port: Int, path: String)

trait AccountManagerClientComponent {
  implicit def asyncContext: ExecutionContext
  implicit val M: Monad[Future]

  def accountManagerFactory(config: Configuration): BasicAccountManager[Future] = {
    val protocol = config[String]("service.protocol")
    val host = config[String]("service.host")
    val port = config[Int]("service.port")
    val path = config[String]("service.path")
    
    val settings = AccountManagerClientSettings(protocol, host, port, path)
    new AccountManagerClient(settings)
  }
}

class AccountManagerClient(settings: AccountManagerClientSettings) extends BasicAccountManager[Future] with AkkaDefaults with Logging {
  import settings._

  val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)
  
  def listAccountIds(apiKey: APIKey) : Future[Set[Account]] = {
    invoke { client =>
      client.query("apiKey", apiKey).contentType(application/MimeTypes.json).get[JValue]("") map {
        case HttpResponse(HttpStatus(OK, _), _, Some(jaccounts), _) =>
         jaccounts.validated[Set[Account]] match {
           case Success(accounts) => accounts
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
  
  def findAccountById(accountId: AccountID): Future[Option[Account]] = {
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

  def close(): Future[Unit] = ().point[Future]
  
  def invoke[A](f: HttpClient[ByteChunk] => A): A = {
    val client = new HttpClientXLightWeb 
    f(client.protocol(protocol).host(host).port(port).path(path))
  }
}
