package com.precog.shard
package service

import blueeyes._
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.DefaultSerialization._

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import com.precog.common.Path
import com.precog.common.security._
import com.precog.common.json._
import com.precog.ingest.service._
import com.precog.daze.QueryOptions
import com.precog.yggdrasil.TableModule
import com.precog.yggdrasil.TableModule._

import scalaz.{ Validation, Success, Failure }
import scalaz.ValidationNEL
import scalaz.Validation._
import scalaz.Monad
import scalaz.syntax.traverse._
import scalaz.std.option._

trait ShardServiceCombinators extends EventServiceCombinators {

  type Query = String

  import DefaultBijections._

  import scalaz.syntax.apply._
  import scalaz.syntax.validation._

  private val Limit = """([1-9][0-9]*)""".r
  private val Offset = """(0|[1-9][0-9]*)""".r

  private def getSortOn(request: HttpRequest[_]): Validation[String, List[CPath]] = {
    request.parameters.get('sortOn).filter(_ != null) map { paths =>
      try {
        val jpaths = JParser.parse(paths)
        jpaths match {
          case JArray(elems) =>
            Validation.success(elems collect { case JString(path) => CPath(path) })
          case JString(path) =>
            Validation.success(CPath(path) :: Nil)
          case badJVal =>
            Validation.failure("The sortOn query parameter was expected to be JSON string or array, but found " + badJVal)
        }
      } catch {
        case ex: ParseException =>
          Validation.failure("Couldn't parse sortOn query parameter: " + ex.getMessage())
      }
    } getOrElse Validation.success[String, List[CPath]](Nil)
  }

  private def getSortOrder(request: HttpRequest[_]): Validation[String, DesiredSortOrder] = {
    request.parameters.get('sortOrder) filter (_ != null) map (_.toLowerCase) map {
      case "asc" | "\"asc\"" | "ascending" | "\"ascending\"" => success(TableModule.SortAscending)
      case "desc" | "\"desc\"" |  "descending" | "\"descending\"" => success(TableModule.SortDescending)
      case badOrder => failure("Unknown sort ordering: %s." format badOrder)
    } getOrElse success(TableModule.SortAscending)
  }

  private def getOffsetAndLimit(request: HttpRequest[_]): ValidationNEL[String, Option[(Int, Int)]] = {
    val limit: Validation[String, Option[Int]] = request.parameters.get('limit).filter(_ != null) map {
      case Limit(str) => Validation.success(Some(str.toInt))
      case _ => Validation.failure("The limit query parameter must be a positive integer.")
    } getOrElse Validation.success(None)

    val offset: Validation[String, Option[Int]] = request.parameters.get('skip).filter(_ != null) map {
      case Offset(str) if limit.map(_.isDefined) | true => Validation.success(Some(str.toInt))
      case Offset(str) => Validation.failure("The offset query parameter cannot be used without a limit.")
      case _ => Validation.failure("The offset query parameter must be a non-negative integer.")
    } getOrElse Validation.success(None)

    (offset.toValidationNEL |@| limit.toValidationNEL) { (offset, limit) =>
      limit map ((offset getOrElse 0, _))
    }
  }

  def query[A, B](next: HttpService[A, (APIKeyRecord, Path, Query, QueryOptions) => Future[B]]) = {
    new DelegatingService[A, (APIKeyRecord, Path) => Future[B], A, (APIKeyRecord, Path, Query, QueryOptions) => Future[B]] {
      val delegate = next
      val metadata = None
      val service = (request: HttpRequest[A]) => {
        val query: Option[String] = request.parameters.get('q).filter(_ != null)
        val offsetAndLimit = getOffsetAndLimit(request)
        val sortOn = getSortOn(request).toValidationNEL
        val sortOrder = getSortOrder(request).toValidationNEL

        (offsetAndLimit |@| sortOn |@| sortOrder) { (offsetAndLimit, sortOn, sortOrder) =>
          val opts = QueryOptions(
            page = offsetAndLimit,
            sortOn = sortOn,
            sortOrder = sortOrder
          )

          query map { q =>
            next.service(request) map { f => (apiKey: APIKeyRecord, path: Path) => f(apiKey, path, q, opts) }
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

  import java.nio.ByteBuffer

  implicit def bbToString(bb: ByteBuffer): String = {
    val arr = new Array[Byte](bb.remaining)
    bb.get(arr)
    new String(arr, "UTF-8")
  }

  implicit def stringToBB(s: String): ByteBuffer = ByteBuffer.wrap(s.getBytes("UTF-8"))

  def jsonpcb[A](delegate: HttpService[Future[JValue], Future[HttpResponse[A]]])
    (implicit bi: A => Future[ByteChunk], M: Monad[Future]) = {

    jsonpc[ByteBuffer, ByteBuffer](delegate map (_ flatMap { response =>
      response.content.map(bi).sequence.map(c0 => response.copy(content = c0))
    }))
  }
}
