package com.precog.shard
package service

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher
import akka.util.Duration

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import com.precog.common._
import com.precog.common.security._
import com.precog.common.security.service._
import com.precog.common.services._
import com.precog.daze._
import com.precog.yggdrasil.TableModule
import com.precog.yggdrasil.TableModule._

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.Validation._
import scalaz.syntax.bifunctor._
import scalaz.syntax.traverse._
import scalaz.syntax.bifunctor._
import scalaz.std.option._

import java.util.concurrent.TimeUnit

trait ShardServiceCombinators extends EitherServiceCombinators with PathServiceCombinators with APIKeyServiceCombinators with Logging {
  type Query = String

  import DefaultBijections._

  import scalaz.syntax.apply._
  import scalaz.syntax.validation._
  import scalaz.std.string._

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

  private def getOutputType(request: HttpRequest[_]): QueryOutput = {
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

    (request.headers.header[Accept].toSeq.flatMap(_.mimeTypes) ++ requestParamType).collectFirst {
      case JSON => JSONOutput
      case CSV => CSVOutput
    } getOrElse {
      JSONOutput
    }
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

  private def getOffsetAndLimit(request: HttpRequest[_]): ValidationNEL[String, Option[(Long, Long)]] = {
    val limit = request.parameters.get('limit).filter(_ != null).map {
      case Limit(n) => Validation.success(n)
      case _ => Validation.failure("The limit query parameter must be a positive integer.")
    }.sequence[({ type λ[α] = Validation[String, α] })#λ, Long]

    val offset = request.parameters.get('skip).filter(_ != null).map {
      case Offset(n) if limit.map(_.isDefined) | true => Validation.success(n)
      case Offset(n) => Validation.failure("The offset query parameter cannot be used without a limit.")
      case _ => Validation.failure("The offset query parameter must be a non-negative integer.")
    }.sequence[({ type λ[α] = Validation[String, α] })#λ, Long]

    (offset.toValidationNEL |@| limit.toValidationNEL) { (offset, limit) =>
      limit map ((offset getOrElse 0, _))
    }
  }

  def query[A, B](next: HttpService[A, (APIKey, Path, Query, QueryOptions) => Future[B]]): HttpService[A, (APIKey, Path) => Future[B]] = {
    new DelegatingService[A, (APIKey, Path) => Future[B], A, (APIKey, Path, Query, QueryOptions) => Future[B]] {
      val delegate = next
      val metadata = None
      val service = (request: HttpRequest[A]) => {
        val query: Option[String] = request.parameters.get('q).filter(_ != null)
        val offsetAndLimit = getOffsetAndLimit(request)
        val sortOn = getSortOn(request).toValidationNEL
        val sortOrder = getSortOrder(request).toValidationNEL
        val timeout = getTimeout(request).toValidationNEL
        val output = getOutputType(request)

        (offsetAndLimit |@| sortOn |@| sortOrder |@| timeout) { (offsetAndLimit, sortOn, sortOrder, timeout) =>
          val opts = QueryOptions(
            page = offsetAndLimit,
            sortOn = sortOn,
            sortOrder = sortOrder,
            timeout = timeout.map(Duration(_, TimeUnit.MILLISECONDS)),
            output = output
          )

          query map { q =>
            next.service(request) map { f => (apiKey: APIKey, path: Path) => f(apiKey, path, q, opts) }
          } getOrElse {
            failure(inapplicable)
          }
        } match {
          case Success(success) => success
          case Failure(errors) => failure(DispatchError(BadRequest, errors.list mkString "\n"))
        }
      }
    }
  }

  def asyncQuery[A, B](next: HttpService[A, (APIKey, Path, Query, QueryOptions) => Future[B]]): HttpService[A, APIKey => Future[B]] = {
    new DelegatingService[A, APIKey => Future[B], A, (APIKey, Path) => Future[B]] {
      val delegate = query[A, B](next)
      val service = { (request: HttpRequest[A]) =>
        val path = request.parameters.get('prefixPath).filter(_ != null).getOrElse("")
        delegate.service(request.copy(parameters = request.parameters + ('sync -> "async"))) map { f =>
          (apiKey: APIKey) => f(apiKey, Path(path))
        }
      }

      def metadata = delegate.metadata
    }
  }

  import java.nio.ByteBuffer

  implicit def bbToString(bb: ByteBuffer): String = {
    val arr = new Array[Byte](bb.remaining)
    bb.get(arr)
    new String(arr, "UTF-8")
  }

  implicit def stringToBB(s: String): ByteBuffer = ByteBuffer.wrap(s.getBytes("UTF-8"))
}
