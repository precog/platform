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
import blueeyes.util.Clock

import com.precog.common.Path
import com.precog.common.ingest.JavaSerialization._
import com.precog.common.security._
import com.precog.common.services.ServiceHandlerUtil._
import com.precog.shard.scheduling._
import com.precog.shard.scheduling.CronExpressionSerialization._
import com.precog.util.PrecogUnit

import com.weiglewilczek.slf4s.Logging

import java.util.UUID

import org.joda.time.DateTime

import org.quartz.CronExpression

import scalaz._
import scalaz.std.option._
import scalaz.std.string._
import scalaz.syntax.apply._
import scalaz.syntax.plus._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.list._
import scalaz.syntax.traverse._

class AddScheduledQueryServiceHandler(scheduleActor: ActorRef, permissionsFinder: PermissionsFinder[Future], clock: Clock)(implicit executor: ExecutionContext, addTimeout: Timeout) extends CustomHttpService[Future[JValue], APIKey => Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[Future[JValue]]) => Success({ apiKey : APIKey =>
    request.content map { futureContent =>
      for {
        body <- futureContent
        taskId = UUID.randomUUID
        taskV <-{
          (
            (body \ "schedule").validated[CronExpression] |@|
            (body \ "ownerAccountIds").validated[List[String]].flatMap { accounts =>
              accounts.toNel.map {
                nea => Success(Authorities(nea))
              } getOrElse Failure(Extractor.Error.invalid("No owner account IDs specified for result storage"))
            } |@|
            (body \ "basePath").validated[Path] |@|
            (body \ "source").validated[Path] |@|
            (body \ "sink").validated[Path] |@|
            (body \ "timeout").validated[Option[Long]]
          ) { ScheduledTask(taskId, _, apiKey, _, _, _, _, _) } map { task =>
            for {
              okToRead  <- permissionsFinder.apiKeyFinder.hasCapability(apiKey, Set(ExecutePermission(task.fqSource)), None)
              okToWrite <- permissionsFinder.checkWriteAuthorities(task.authorities, apiKey, task.fqSink, clock.instant)
            } yield {
              List(
                (!okToRead) option ("The provided API Key does not have permission to execute " + task.fqSource),
                (!okToWrite) option ("The provided API Key does not have permission to write to %s as %s".format(task.fqSink, task.authorities.render))
              ).flatten match {
                case Nil => Success(task)
                case errors => Failure(forbidden(errors.mkString(", ")))
              }
            }
          } valueOr { errors =>
            Promise successful Failure(badRequest(errors.message))
          }
        }
        addResultV <- taskV match {
          case Success(task) =>
            (scheduleActor ? AddTask(task)).mapTo[Validation[String, PrecogUnit]] map {
              _ leftMap { error =>
                logger.error("Failure adding scheduled execution: " + error)
                HttpResponse(status = HttpStatus(InternalServerError), content = Some("An error occurred scheduling your query".serialize))
              }
            }

          case f @ Failure(_) =>
            Promise successful f
        }
      } yield {
        addResultV map { _ =>
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

class DeleteScheduledQueryServiceHandler[A](scheduleActor: ActorRef)(implicit executor: ExecutionContext, deleteTimeout: Timeout) extends CustomHttpService[A, Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[A]) => Success({
    request.parameters.get('scheduleId) map { idStr =>
      Validation.fromTryCatch { UUID.fromString(idStr) } map { id =>
        (scheduleActor ? DeleteTask(id)).mapTo[Validation[String, PrecogUnit]] map {
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

class ScheduledQueryStatusServiceHandler[A](scheduleActor: ActorRef, permissionsFinder: PermissionsFinder[Future], clock: Clock)(implicit executor: ExecutionContext, addTimeout: Timeout) extends CustomHttpService[A, Future[HttpResponse[JValue]]] with Logging {
  val service = (request: HttpRequest[A]) => Success({
    request.parameters.get('scheduleId) map { idStr =>
      Validation.fromTryCatch { request.parameters.get('last) map(_.toInt) } leftMap {
        case ex: NumberFormatException => "Invalid last limit: " + ex.getMessage
      } flatMap { limit =>
        Validation.fromTryCatch { UUID.fromString(idStr) } map { id =>
          (scheduleActor ? StatusForTask(id, limit)).mapTo[Validation[String, Option[(ScheduledTask, Seq[ScheduledRunReport])]]] map {
            _ map {
              case Some((task, reports)) =>
                val body: JValue = JObject(
                  "task" -> task.serialize,
                  "nextRun" -> (Option(task.schedule.getNextValidTimeAfter(new java.util.Date)) map { d => new DateTime(d).serialize } getOrElse { JString("never") }),
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
