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

case class AddScheduledQueryRequest(schedule: CronExpression, owners: Set[AccountId], basePath: Path, source: Path, sink: Path, timeout: Option[Long])

object AddScheduledQueryRequest {
  import CronExpressionSerialization._

  implicit val iso = Iso.hlist(AddScheduledQueryRequest.apply _, AddScheduledQueryRequest.unapply _)

  val schemaV1 = "schedule" :: "owners" :: "basePath" :: "source" :: "sink" :: "timeout" :: HNil

  implicit val decomposer: Decomposer[AddScheduledQueryRequest] = decomposerV(schemaV1, Some("1.0".v))
  implicit val extractor:  Extractor[AddScheduledQueryRequest]  = extractorV(schemaV1, Some("1.0".v))
}

class AddScheduledQueryServiceHandler(scheduler: Scheduler[Future], apiKeyFinder: APIKeyFinder[Future], accountFinder: AccountFinder[Future], clock: Clock)(implicit M: Monad[Future], executor: ExecutionContext, addTimeout: Timeout) extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
  val service: HttpRequest[Future[JValue]] => Validation[NotServed, APIKey => Future[HttpResponse[JValue]]] = (request: HttpRequest[Future[JValue]]) => Success({ apiKey : APIKey =>
    val permissionsFinder = new PermissionsFinder[Future](apiKeyFinder, accountFinder, clock.instant)
    request.content map { contentFuture =>
      for {
        content <- contentFuture
        sreqV = content.validated[AddScheduledQueryRequest] leftMap { error => 
          badRequest("Request body %s is not a valid scheduling query request: %s".format(content.renderCompact, error.message))
        }
        taskVV <- sreqV traverse { schedulingRequest =>
          Authorities.ifPresent(schedulingRequest.owners) map { authorities =>
            for {
              okToReads <- authorities.accountIds.toStream traverse { accountId =>
                             permissionsFinder.apiKeyFinder.hasCapability(apiKey, Set(ExecutePermission(schedulingRequest.source, WrittenBy(accountId))), None)
                           }
              okToRead   = okToReads.exists(_ == true)
              okToWrite <- permissionsFinder.checkWriteAuthorities(authorities, apiKey, schedulingRequest.sink, clock.instant)
            } yield {
              val readError = (!okToRead).option(nels("The API Key does not have permission to execute %s".format(schedulingRequest.source.path))) 
              val writeError = (!okToWrite).option(nels("The API Key does not have permission to write to %s as %s".format(schedulingRequest.sink.path, authorities.render)))
              (readError |+| writeError) map {
                errors => forbidden(errors.list.mkString(", "))
              } toFailure {
                (schedulingRequest, authorities)
              }
            }
          } getOrElse {
            Promise successful Failure(badRequest("You must provide at least one owner account for the task results!"))
          }
        } 
        taskV: Validation[HttpResponse[JValue], (AddScheduledQueryRequest, Authorities)] = taskVV.flatMap (x => x) 
        addResultVV <- taskV traverse {
          case (request, authorities) =>
            scheduler.addTask(Some(request.schedule), apiKey, authorities, request.basePath, request.source, request.sink, request.timeout) map {
              _ leftMap { error =>
                logger.error("Failure adding scheduled execution: " + error)
                HttpResponse(status = HttpStatus(InternalServerError), content = Some("An error occurred scheduling your query".serialize))
              }
            }
        }
      } yield {
        val addResultV: Validation[HttpResponse[JValue], UUID] = addResultVV.flatMap(a => a) 
        addResultV map { taskId: UUID =>
          HttpResponse(content = Some(taskId.serialize))
        } valueOr {
          errResponse => errResponse
        }
      }
    } getOrElse {
      Promise successful badRequest("Missing body for scheduled query submission")
    }
  })

  val metadata = DescriptionMetadata("Add a new scheduled query")
}

class DeleteScheduledQueryServiceHandler[A](scheduler: Scheduler[Future])(implicit executor: ExecutionContext, deleteTimeout: Timeout) extends CustomHttpService[A, Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[A]) => Success({
    request.parameters.get('scheduleId) map { idStr =>
      Validation.fromTryCatch { UUID.fromString(idStr) } map { id =>
        scheduler.deleteTask(id) map {
          case Success(_) => ok[String](None)
          case Failure(error) => HttpResponse(status = HttpStatus(InternalServerError), content = Some("An error occurred deleting your query".serialize))
        }
      } valueOr { _ =>
        Promise successful badRequest("Invalid schedule Id \"%s\"".format(idStr))
      }
    } getOrElse {
      Promise successful badRequest("Missing schedule Id for deletion")
    }
  })

  val metadata = DescriptionMetadata("Delete a scheduled entry")
}

class ScheduledQueryStatusServiceHandler[A](scheduler: Scheduler[Future])(implicit executor: ExecutionContext, addTimeout: Timeout) extends CustomHttpService[A, Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[A]) => Success({
    request.parameters.get('scheduleId) map { idStr =>
      Validation.fromTryCatch { request.parameters.get('last) map(_.toInt) } leftMap {
        case ex: NumberFormatException => "Invalid last limit: " + ex.getMessage
      } flatMap { limit =>
        Validation.fromTryCatch { UUID.fromString(idStr) } map { id =>
          scheduler.statusForTask(id, limit) map {
            _ map {
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
        } leftMap { _ =>
          "Invalid schedule Id \"%s\"".format(idStr)
        }
      } valueOr { error =>
        Promise successful badRequest(error)
      }

    } getOrElse {
      Promise successful badRequest("Missing schedule Id for status")
    }
  })

  val metadata = DescriptionMetadata("Query the status of a scheduled entry")
}

//type ScheduleServiceHandlers
