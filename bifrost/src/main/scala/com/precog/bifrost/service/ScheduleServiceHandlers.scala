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
package com.precog.bifrost
package service

import akka.actor.ActorRef
import akka.dispatch.{ExecutionContext, Future, Promise}
import akka.pattern.ask
import akka.util.Timeout

import blueeyes.core.http.{Success => _, _}
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization.{DateTimeDecomposer => _, _}
import blueeyes.json.serialization.Versioned._
import blueeyes.util.Clock

import com.precog.common.Path
import com.precog.common.accounts._
import com.precog.common.ingest.JavaSerialization._
import com.precog.common.security._
import com.precog.common.services.ServiceHandlerUtil._
import com.precog.yggdrasil.scheduling._
import com.precog.yggdrasil.scheduling.CronExpressionSerialization._
import com.precog.util.PrecogUnit
import Permission._
import com.precog.yggdrasil.execution.EvaluationContext

import com.weiglewilczek.slf4s.Logging

import java.util.UUID

import org.joda.time.DateTime

import org.quartz.CronExpression

import scalaz._
import scalaz.NonEmptyList._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.std.stream._
import scalaz.syntax.apply._
import scalaz.syntax.plus._
import scalaz.syntax.semigroup._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.list._
import scalaz.syntax.std.option._
import scalaz.syntax.traverse._

import shapeless._

case class AddScheduledQueryRequest(schedule: CronExpression, owners: Set[AccountId], context: EvaluationContext, source: Path, sink: Path, timeout: Option[Long])

object AddScheduledQueryRequest {
  import CronExpressionSerialization._

  implicit val iso = Iso.hlist(AddScheduledQueryRequest.apply _, AddScheduledQueryRequest.unapply _)

  val schemaV1 = "schedule" :: "owners" :: "context" :: "source" :: "sink" :: "timeout" :: HNil

  implicit val decomposer: Decomposer[AddScheduledQueryRequest] = decomposerV(schemaV1, Some("1.0".v))
  implicit val extractor:  Extractor[AddScheduledQueryRequest]  = extractorV(schemaV1, Some("1.0".v))
}

class AddScheduledQueryServiceHandler(scheduler: Scheduler[Future], apiKeyFinder: APIKeyFinder[Future], accountFinder: AccountFinder[Future], clock: Clock)(implicit M: Monad[Future], executor: ExecutionContext, addTimeout: Timeout) extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, APIKey => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => Success({ apiKey : APIKey =>
    val permissionsFinder = new PermissionsFinder[Future](apiKeyFinder, accountFinder, clock.instant)
    request.content map { contentFuture =>
      val responseVF = for {
        sreq <- EitherT {
          contentFuture map { jv =>
            jv.validated[AddScheduledQueryRequest].disjunction leftMap { err => 
              badRequest("Request body %s is not a valid scheduling query request: %s".format(jv.renderCompact, err.message))
            }
          }
        }

        authorities <- EitherT { 
          M point {
            (Authorities.ifPresent(sreq.owners) \/> badRequest("You must provide an owner account for the task results!"))
          }
        }

        okToReads <- EitherT.right {
          authorities.accountIds.toStream traverse { acctId =>
            permissionsFinder.apiKeyFinder.hasCapability(apiKey, Set(ExecutePermission(sreq.source, WrittenBy(acctId))), None)
          }
        }
        okToRead   = okToReads.exists(_ == true)
        okToWrite <- EitherT.right(permissionsFinder.checkWriteAuthorities(authorities, apiKey, sreq.sink, clock.instant))

        readError  = (!okToRead).option(nels("The API Key does not have permission to execute %s".format(sreq.source.path))) 
        writeError = (!okToWrite).option(nels("The API Key does not have permission to write to %s as %s".format(sreq.sink.path, authorities.render)))

        taskId    <- (readError |+| writeError) match {
          case None =>
            scheduler.addTask(Some(sreq.schedule), apiKey, authorities, sreq.context, sreq.source, sreq.sink, sreq.timeout) leftMap { error =>
              logger.error("Failure adding scheduled execution: " + error)
              HttpResponse(status = HttpStatus(InternalServerError), content = Some("An error occurred scheduling your query".serialize))
            }

          case Some(errors) => 
            EitherT.left(M point forbidden(errors.list.mkString(", ")))
        }
      } yield {
        HttpResponse(content = Some(taskId.serialize))
      }

      responseVF.fold(a => a, a => a)
    } getOrElse {
      Promise successful badRequest("Missing body for scheduled query submission")
    }
  })

  val metadata = DescriptionMetadata("Add a new scheduled query")
}

class DeleteScheduledQueryServiceHandler[A](scheduler: Scheduler[Future])(implicit executor: ExecutionContext, deleteTimeout: Timeout) extends CustomHttpService[A, Future[HttpResponse[JValue]]] with Logging {
  import com.precog.util._
  val service = (request: HttpRequest[A]) => {
    for {
      idStr <- request.parameters.get('scheduleId).toSuccess(DispatchError(BadRequest, "scheduleId parameter required")) 
      id <- Validation.fromTryCatch { UUID.fromString(idStr) } leftMap { error => DispatchError(BadRequest, "Invalid schedule Id \"%s\"".format(idStr)) }
    } yield {
      scheduler.deleteTask(id) map { _ => 
        ok[String](None)
      } valueOr { error =>
        sys.error("todo")
        //serverError("An error occurred deleting your query", Some(error))
      }
    }
  }

  val metadata = DescriptionMetadata("Delete a scheduled entry")
}

class ScheduledQueryStatusServiceHandler[A](scheduler: Scheduler[Future])(implicit executor: ExecutionContext, addTimeout: Timeout) extends CustomHttpService[A, Future[HttpResponse[JValue]]] with Logging {
  import com.precog.util._
  val service = (request: HttpRequest[A]) => {
    for {
      idStr <- request.parameters.get('scheduleId).toSuccess(DispatchError(BadRequest, "Missing schedule Id for status."))
      id <- Validation.fromTryCatch { UUID.fromString(idStr) } leftMap { ex => DispatchError(BadRequest, "Invalid schedule Id \"%s\"".format(idStr), Some(ex.getMessage)) }
      limit <- Validation.fromTryCatch { request.parameters.get('last) map(_.toInt) } leftMap {
        case ex: NumberFormatException => DispatchError(BadRequest, "Invalid last limit: " + ex.getMessage)
      } 
    } yield {
      scheduler.statusForTask(id, limit) map {
        case Some((task, reports)) =>
          val nextTime: Option[DateTime] = task.repeat.flatMap { 
            sched: CronExpression => Option(sched.getNextValidTimeAfter(new java.util.Date))
          } map { d => new DateTime(d) }

          val body: JValue = JObject(
            "task" -> task.serialize,
            "nextRun" -> (nextTime.map (_.serialize) getOrElse { JString("never") }),
            "history" -> reports.toList.serialize
          )

          ok(Some(body))

        case None =>
          notFound("No status found for id " + idStr)

      } valueOr { error =>
        HttpResponse(status = HttpStatus(InternalServerError), content = Some("An error occurred getting status for your query".serialize))
      }
    }
  }

  val metadata = DescriptionMetadata("Query the status of a scheduled entry")
}

//type ScheduleServiceHandlers
