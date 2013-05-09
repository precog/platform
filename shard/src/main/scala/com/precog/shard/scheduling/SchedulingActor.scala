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
package scheduling

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable}
import akka.dispatch.{Await, Future}
import akka.pattern.{ask, pipe}
import akka.util.{Duration, Timeout}

import blueeyes.bkka.FutureMonad
import blueeyes.util.Clock

import com.precog.common.accounts.AccountFinder
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.daze.QueryOptions
import com.precog.muspelheim.Platform
import com.precog.util.PrecogUnit
import com.precog.yggdrasil.table.Slice
import com.precog.yggdrasil.vfs._

import com.weiglewilczek.slf4s.Logging

import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit

import org.joda.time.{Duration => JodaDuration, DateTime}

import org.quartz.CronExpression

import scala.collection.mutable.{ArrayBuffer, PriorityQueue}

import scalaz.{Ordering => _, idInstance => _, _}
import scalaz.syntax.traverse._
import scalaz.effect.IO

sealed trait SchedulingMessage

case class AddTask(task: ScheduledTask)

case class DeleteTask(id: UUID)

case class StatusForTask(id: UUID, limit: Option[Int])

case object WakeForRun extends SchedulingMessage

case class TaskComplete(id: UUID, endedAt: DateTime, total: Long) extends SchedulingMessage

case class TaskFailed(id: UUID, error: String) extends SchedulingMessage


class SchedulingActor(jobManager: JobManager[Future], storage: ScheduleStorage[Future], projectionsActor: ActorRef, platform: Platform[Future, StreamT[Future, Slice]], apiKeyFinder: APIKeyFinder[Future], accountFinder: AccountFinder[Future], clock: Clock, storageTimeout: Duration = Duration(30, TimeUnit.SECONDS), resourceTimeout: Timeout = Timeout(10, TimeUnit.SECONDS)) extends Actor with Logging {
  private[this] final implicit val scheduleOrder: Ordering[(DateTime, ScheduledTask)] = Ordering.by(_._1.getMillis)

  private[this] implicit val M: Monad[Future] = new FutureMonad(context.dispatcher)

  private[this] val scheduleQueue = PriorityQueue.empty[(DateTime, ScheduledTask)]

  private[this] var scheduledAwake: Option[Cancellable] = None

  private case class TaskInProgress(task: ScheduledTask, startedAt: DateTime)

  private[this] var running = Map.empty[UUID, TaskInProgress]

  override def preStart = {
    val now = new Date

    scheduleQueue ++= Await.result(storage.listTasks map {
      _ flatMap(nextRun(now, _))
    }, storageTimeout)

    scheduleNextTask()
  }

  override def postStop = {
    scheduledAwake foreach { sa =>
      if (! sa.isCancelled) {
        sa.cancel()
      }
    }
  }

  def scheduleNextTask() = {
    // Just make sure we don't multi-schedule
    scheduledAwake foreach { sa =>
      if (! sa.isCancelled) {
        sa.cancel()
      }
    }

    scheduleQueue.headOption foreach { head =>
      val delay = Duration(new JodaDuration(new DateTime, head._1).getMillis, TimeUnit.MILLISECONDS)

      scheduledAwake = Some(context.system.scheduler.scheduleOnce(delay, self, WakeForRun))
    }
  }

  def nextRun(threshold: Date, task: ScheduledTask) = {
    Option(task.schedule.getNextValidTimeAfter(threshold)) map { nextTime =>
      (new DateTime(nextTime), task)
    }
  }

  def rescheduleTask(task: ScheduledTask) = {
    nextRun(new Date, task) match {
      case Some(next) =>
        scheduleQueue += next
        scheduleNextTask()

      case None => logger.warn("No further run times for " + task)
    }
  }

  def executeTask(task: ScheduledTask): Future[PrecogUnit] = {
    val ourself = self
    val startedAt = new DateTime

    implicit val readTimeout = resourceTimeout

    // This cannot occur inside a Future, or we would be exposing Actor state outside of this thread
    running += (task.id -> TaskInProgress(task, startedAt))

    (for {
      job <- jobManager.createJob(task.apiKey, task.taskName, "scheduled", None, Some(startedAt))
      executorV <- platform.executorFor(task.apiKey)
      scriptV <- {
        (projectionsActor ? Read(task.source, None, Some(task.apiKey))).mapTo[ReadResult] map {
          case ReadSuccess(_, Some(blob: Blob)) =>
            blob.asString map(Success(_)) except {
              case t: Throwable =>
                IO(Failure("Execution failed reading source script: " + Option(t.getMessage).getOrElse(t.getClass.toString)))
            } unsafePerformIO

          case ReadSuccess(_, Some(_)) =>
            Failure("Execution failed on non-script source")

          case ReadSuccess(_, None) =>
            Failure("Execution failed on non-existent source script")

          case ReadFailure(_, errors) =>
            Failure("Execution failed retrieving source script:\n  " + errors.list.mkString("\n  "))
        }
      }
      execution <- (for {
        script <- scriptV
        executor <- executorV
      } yield {
        import task._

        val permissionsFinder = new PermissionsFinder(apiKeyFinder, accountFinder, clock.instant)

        permissionsFinder.writePermissions(apiKey, fqSink, clock.instant()) flatMap { perms =>
          val allPerms = Map(apiKey -> perms.toSet[Permission])

          executor.execute(apiKey, script, prefix, QueryOptions(timeout = task.timeout)).flatMap {
            _.traverse { stream =>
              QueryResultConvert.toIngest(stream, fqSink, apiKey, authorities, Some(job.id), clock).foldLeft((0, 0L)) {
                case ((offset, total), (len, msg)) =>
                  projectionsActor ! IngestBundle(Seq((offset, msg)), allPerms)
                  (offset + 1, total + len)
              } map { case (_, total) =>
                ourself ! TaskComplete(task.id, new DateTime, total)
                total
              }
            }
          }
        }
      }) valueOr { error =>
        jobManager.abort(job.id, error) map { _ =>
          ourself ! TaskFailed(task.id, error)
        }
      }
    } yield PrecogUnit) onFailure {
      case t: Throwable =>
        ourself ! TaskFailed(task.id, Option(t.getMessage) getOrElse t.getClass.toString)
        PrecogUnit
    }
  }

  def receive = {
    case AddTask(task) =>
      val ourself = self
      storage.addTask(task) map { pu =>
        ourself ! WakeForRun
        Success(pu)
      } recover {
        case t: Throwable =>
          logger.error("Error adding task " + task, t)
          Failure("Internal error adding task")
      } pipeTo sender

    case DeleteTask(id) =>
      val ourself = self
      storage.deleteTask(id) map { pu =>
        ourself ! WakeForRun
        Success(pu)
      } recover {
        case t: Throwable =>
          logger.error("Error deleting task " + id, t)
          Failure("Internal error deleting task")
      } pipeTo sender

    case StatusForTask(id, limit) =>
      storage.statusFor(id, limit) map(Success(_)) recover {
        case t: Throwable =>
          logger.error("Error getting status for task " + id, t)
          Failure("Internal error getting status for task")
      } pipeTo sender

    case WakeForRun =>
      val now = new DateTime
      val torun = ArrayBuffer.empty[ScheduledTask]
      while (!scheduleQueue.isEmpty && !scheduleQueue.head._1.isAfter(now)) {
        torun += scheduleQueue.dequeue._2
      }
      torun.foreach(executeTask)

      scheduleNextTask()

    case TaskComplete(id, endedAt, total) =>
      running.get(id) match {
        case Some(TaskInProgress(task, startAt)) =>
          logger.info("Scheduled task %s completed with %d records in %d millis".format(id, total, (new JodaDuration(startAt, endedAt)).getMillis))
          storage.reportRun(ScheduledRunReport(id, startAt, endedAt, total))
          rescheduleTask(task)
          running -= id

        case None =>
          logger.error("Task completion reported for unknown task " + id)
      }

    case TaskFailed(id, error) =>
      running.get(id) match {
        case Some(TaskInProgress(task, startAt)) =>
          val now = new DateTime
          logger.warn("Scheduled task %s failed after %d millis: %s".format(id, (new JodaDuration(startAt, now)).getMillis, error))
          storage.reportRun(ScheduledRunReport(id, startAt, now, 0, List(error)))
          rescheduleTask(task)
          running -= id

        case None =>
          logger.error("Task failure reported for unknown task " + id)
      }
  }
}
