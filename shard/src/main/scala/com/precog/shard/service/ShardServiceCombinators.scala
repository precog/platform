package com.precog.shard
package service

import akka.dispatch.Future
import akka.dispatch.Promise
import akka.dispatch.ExecutionContext
import akka.util.Duration

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.HttpHeaders._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import com.precog.common._
import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.accounts._
import com.precog.common.services._
import com.precog.daze._
import com.precog.yggdrasil.execution._
import com.precog.yggdrasil.TableModule
import com.precog.yggdrasil.TableModule._

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.Validation._
import scalaz.std.string._
import scalaz.std.option._
import scalaz.syntax.apply._
import scalaz.syntax.bifunctor._
import scalaz.syntax.traverse._
import scalaz.syntax.validation._
import scalaz.syntax.std.boolean._

import java.util.concurrent.TimeUnit

object ShardServiceCombinators extends Logging {

  trait NonNegativeLong {
    val BigIntPattern = """(0|[1-9][0-9]*)""".r

    def unapply(str: String): Option[Long] = str match {
      case BigIntPattern(num) =>
        val big = BigInt(num)
        val n = big.toLong
        if (big == BigInt(n)) Some(n) else None
      case _ => None
    }
  }

  private object Limit extends NonNegativeLong {
    override def unapply(str: String): Option[Long] = super.unapply(str) filter (_ > 0)
  }
  private object Offset extends NonNegativeLong
  private object Millis extends NonNegativeLong

  private def getOutputType(request: HttpRequest[_]): MimeType = {
    import MimeTypes._
    import HttpHeaders.Accept

    val JSON = application/json
    val CSV = text/csv

    val requestParamType = for {
      headerString <- request.parameters.get('headers)
      headerJson <- JParser.parseFromString(headerString).toOption
      headerMap <- headerJson.validated[Map[String, String]].toOption
      contentTypeString <- headerMap.get("Accept")
      accept <- Accept.parse(contentTypeString)
      mimeType <- accept.mimeTypes.headOption
    } yield mimeType

    val requested = (request.headers.header[Accept].toSeq.flatMap(_.mimeTypes) ++ requestParamType)

    val allowed = Set(JSON, CSV, anymaintype/anysubtype)

    requested.find(allowed).headOption.getOrElse(JSON)
  }

  private def getTimeout(request: HttpRequest[_]): Validation[String, Option[Long]] = {
    request.parameters.get('timeout).filter(_ != null).map {
      case Millis(n) => Validation.success(n)
      case _ => Validation.failure("Timeout must be a non-negative integer.")
    }.sequence[({ type λ[α] = Validation[String, α] })#λ, Long]
  }

  private def getSortOn(request: HttpRequest[_]): Validation[String, List[CPath]] = {
    import blueeyes.json.serialization.Extractor._
    val onError: Error => String = {
      case err @ Thrown(ex) =>
        logger.warn("Exceptiion thrown from JSON parsing of sortOn parameter", ex)
        err.message
      case other =>
        other.message
    }

    request.parameters.get('sortOn).filter(_ != null) map { paths =>
      val parsed: Validation[Error, List[CPath]] = ((Thrown(_:Throwable)) <-: JParser.parseFromString(paths)) flatMap {
        case JArray(elems) =>
          Validation.success(elems collect { case JString(path) => CPath(path) })
        case JString(path) =>
          Validation.success(CPath(path) :: Nil)
        case badJVal =>
          Validation.failure(Invalid("The sortOn query parameter was expected to be JSON string or array, but found " + badJVal))
      }

      onError <-: parsed
    } getOrElse {
      Validation.success[String, List[CPath]](Nil)
    }
  }

  private def getSortOrder(request: HttpRequest[_]): Validation[String, DesiredSortOrder] = {
    request.parameters.get('sortOrder) filter (_ != null) map (_.toLowerCase) map {
      case "asc" | "\"asc\"" | "ascending" | "\"ascending\"" => success(TableModule.SortAscending)
      case "desc" | "\"desc\"" |  "descending" | "\"descending\"" => success(TableModule.SortDescending)
      case badOrder => failure("Unknown sort ordering: %s." format badOrder)
    } getOrElse success(TableModule.SortAscending)
  }

  private def getOffsetAndLimit(request: HttpRequest[_]): ValidationNel[String, Option[(Long, Long)]] = {
    val limit = request.parameters.get('limit).filter(_ != null).map {
      case Limit(n) => Validation.success(n)
      case _ => Validation.failure("The limit query parameter must be a positive integer.")
    }.sequence[({ type λ[α] = Validation[String, α] })#λ, Long]

    val offset = request.parameters.get('skip).filter(_ != null).map {
      case Offset(n) if limit.map(_.isDefined) | true => Validation.success(n)
      case Offset(n) => Validation.failure("The offset query parameter cannot be used without a limit.")
      case _ => Validation.failure("The offset query parameter must be a non-negative integer.")
    }.sequence[({ type λ[α] = Validation[String, α] })#λ, Long]

    (offset.toValidationNel |@| limit.toValidationNel) { (offset, limit) =>
      limit map ((offset getOrElse 0, _))
    }
  }

  def queryOpts(request: HttpRequest[_]) = {
    val offsetAndLimit = getOffsetAndLimit(request)
    val sortOn = getSortOn(request).toValidationNel
    val sortOrder = getSortOrder(request).toValidationNel
    val timeout = getTimeout(request).toValidationNel
    val output = getOutputType(request)

    (offsetAndLimit |@| sortOn |@| sortOrder |@| timeout) { (offsetAndLimit, sortOn, sortOrder, timeout) =>
      QueryOptions(offsetAndLimit, sortOn, sortOrder, timeout.map(Duration(_, TimeUnit.MILLISECONDS)), output)
    } leftMap { errors =>
      DispatchError(BadRequest, "Errors were encountered decoding query configuration parameters.", Some(errors.list mkString "\n")) : NotServed
    }
  }

}

trait ShardServiceCombinators extends EitherServiceCombinators with PathServiceCombinators with APIKeyServiceCombinators with Logging {
  import ShardServiceCombinators._
  import ServiceHandlerUtil._
  import DefaultBijections._

  type Query = String

  def query[B](next: HttpService[ByteChunk, (APIKey, AccountDetails, Path, Query, QueryOptions) => Future[HttpResponse[B]]])(implicit executor: ExecutionContext): HttpService[ByteChunk, ((APIKey, AccountDetails), Path) => Future[HttpResponse[B]]] = {
    new DelegatingService[ByteChunk, ((APIKey, AccountDetails), Path) => Future[HttpResponse[B]], ByteChunk, (APIKey, AccountDetails, Path, Query, QueryOptions) => Future[HttpResponse[B]]] {
      val delegate = next
      val metadata = NoMetadata
      val service: HttpRequest[ByteChunk] => Validation[NotServed, ((APIKey, AccountDetails), Path) => Future[HttpResponse[B]]] = (request: HttpRequest[ByteChunk]) => {
        queryOpts(request) flatMap { opts =>
          def quirrelContent(request: HttpRequest[ByteChunk]): Option[ByteChunk] = for {
            header <- request.headers.header[`Content-Type`] if header.mimeTypes exists { t =>
                        t == text/plain || (t.maintype == "text" && t.subtype == "x-quirrel-script")
                      }
            content <- request.content
          } yield content

          next.service(request) map { f => 
            val serv: ((APIKey, AccountDetails), Path) => Future[HttpResponse[B]] = { case ((apiKey, account), path) =>
              val query: Option[Future[String]] = 
                request.parameters.get('q).filter(_ != null).map(Promise.successful).
                orElse(quirrelContent(request).map(ByteChunk.forceByteArray(_:ByteChunk).map(new String(_: Array[Byte], "UTF-8"))))

              val result: Future[HttpResponse[B]] = query map { q =>
                q flatMap { f(apiKey, account, path, _: String, opts) }
              } getOrElse {
                Promise.successful(HttpResponse(HttpStatus(BadRequest, "Neither the query string nor request body contained an identifiable quirrel query.")))
              }
              result
            }
            serv
          }
        }
      }
    }
  }

  def asyncQuery[B](next: HttpService[ByteChunk, (APIKey, AccountDetails, Path, Query, QueryOptions) => Future[HttpResponse[B]]])(implicit executor: ExecutionContext): HttpService[ByteChunk, ((APIKey, AccountDetails)) => Future[HttpResponse[B]]] = {
    new DelegatingService[ByteChunk, ((APIKey, AccountDetails)) => Future[HttpResponse[B]], ByteChunk, ((APIKey, AccountDetails), Path) => Future[HttpResponse[B]]] {
      val delegate = query[B](next)
      val service = { (request: HttpRequest[ByteChunk]) =>
        val path = request.parameters.get('prefixPath).filter(_ != null).getOrElse("")
        delegate.service(request.copy(parameters = request.parameters + ('sync -> "async"))) map { f =>
          { (cred: (APIKey, AccountDetails)) => f(cred, Path(path)) }
        }
      }

      def metadata = delegate.metadata
    }
  }

  def requireAccount[A, B](accountFinder: AccountFinder[Future])
      (service: HttpService[A, ((APIKey, AccountDetails)) => Future[HttpResponse[B]]])
      (implicit inj: JValue => B, M: Monad[Future]): HttpService[A, APIKey => Future[HttpResponse[B]]] = {
    val service0 = service map { (f: ((APIKey, AccountDetails)) => Future[HttpResponse[B]]) =>
      { (v: Validation[String, (APIKey, AccountDetails)]) =>
        v.fold(msg => M.point(forbidden(msg) map inj), f)
      }
    }
    new FindAccountService(accountFinder)(service0)
  }
}

final class FindAccountService[A, B](accountFinder: AccountFinder[Future])
    (val delegate: HttpService[A, Validation[String, (APIKey, AccountDetails)] => Future[B]])
    (implicit M: Monad[Future]) extends
    DelegatingService[A, APIKey => Future[B], A, Validation[String, (APIKey, AccountDetails)] => Future[B]] {

  val service: HttpRequest[A] => Validation[NotServed, APIKey => Future[B]] = { (request: HttpRequest[A]) =>
    delegate.service(request) map { (f: Validation[String, (APIKey, AccountDetails)] => Future[B]) =>
      { (apiKey: APIKey) =>
        val details = OptionT(accountFinder.findAccountByAPIKey(apiKey)) flatMap { accountId =>
          OptionT(accountFinder.findAccountDetailsById(accountId))
        }
        val result = details.fold(account => Success((apiKey, account)), Failure("Cannot find account for API key: " + apiKey))
        result flatMap f
      }
    }
  }

  val metadata = DescriptionMetadata("Finds the account associated with a given API key.")
}
