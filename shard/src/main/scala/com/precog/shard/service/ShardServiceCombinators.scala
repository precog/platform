/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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
import com.precog.daze.{QueryOptions, QueryOutput, JsonOutput, CsvOutput}
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

  // XYZ
  private def getOutputType(request: HttpRequest[_]): QueryOutput = {
    import MimeTypes._
    import HttpHeaders.Accept

    val JSON = application/json
    val CSV = text/csv
    val xyz = request.headers.header[Accept].map(_.mimeTypes).getOrElse(Nil)
    System.err.println(xyz.toString)

    val r = xyz.collect {
      case JSON =>
        System.err.println("saw json!!!")
        JsonOutput
      case CSV =>
        System.err.println("saw csv!!!")
        CsvOutput
    }.headOption.getOrElse(JsonOutput)
    System.err.println("returning %s" format r)
    r
  }

  private def getTimeout(request: HttpRequest[_]): Validation[String, Option[Long]] = {
    request.parameters.get('timeout).filter(_ != null).map {
      case Millis(n) => Validation.success(n)
      case _ => Validation.failure("Timeout must be a non-negative integer.")
    }.sequence[({ type λ[α] = Validation[String, α] })#λ, Long]
  }

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

  def query[A, B](next: HttpService[A, (APIKeyRecord, Path, Query, QueryOptions) => Future[B]]) = {
    new DelegatingService[A, (APIKeyRecord, Path) => Future[B], A, (APIKeyRecord, Path, Query, QueryOptions) => Future[B]] {
      val delegate = next
      val metadata = None
      System.err.println("query setup")
      val service = (request: HttpRequest[A]) => {
        System.err.println("query service")
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
            timeout = timeout,
            output = output
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

  def asyncQuery[A, B](next: HttpService[A, (APIKeyRecord, Path, Query, QueryOptions) => Future[B]]) = {
    new DelegatingService[A, APIKeyRecord => Future[B], A, (APIKeyRecord, Path) => Future[B]] {
      val delegate = query(next)
      val service = { (request: HttpRequest[A]) =>
        val path = request.parameters.get('prefixPath).filter(_ != null).getOrElse("")
        delegate.service(request.copy(parameters = request.parameters + ('sync -> "async"))) map { f =>
          (apiKey: APIKeyRecord) => f(apiKey, Path(path))
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

  def jsonpcb[A](delegate: HttpService[Future[JValue], Future[HttpResponse[A]]])
    (implicit bi: A => Future[ByteChunk], M: Monad[Future]) = {

    jsonpc[ByteBuffer, ByteBuffer](delegate map (_ flatMap { response =>
      response.content.map(bi).sequence.map(c0 => response.copy(content = c0))
    }))
  }
}
