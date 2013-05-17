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
package com.precog.muspelheim
package scheduling

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable}
import akka.dispatch.{Await, Future, Promise}
import akka.pattern.{ask, pipe}
import akka.util.{Duration, Timeout}

import blueeyes.bkka.FutureMonad
import blueeyes.util.Clock

import com.precog.common.Path
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
import scalaz.syntax.id._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._
import scalaz.effect.IO

sealed trait SchedulingMessage

case class AddTask(repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, prefix: Path, source: Path, sink: Path, timeoutMillis: Option[Long])

case class DeleteTask(id: UUID)

case class StatusForTask(id: UUID, limit: Option[Int])

object SchedulingActor {
  type TaskKey = (Path, Path)

  private[SchedulingActor] case object WakeForRun extends SchedulingMessage

  private[SchedulingActor] case class AddTasksToQueue(tasks: Seq[ScheduledTask]) extends SchedulingMessage

  private[SchedulingActor] case class RemoveTaskFromQueue(id: UUID) extends SchedulingMessage

  private[SchedulingActor] case class TaskComplete(id: UUID, endedAt: DateTime, total: Long, error: Option[String]) extends SchedulingMessage

  private[SchedulingActor] case class TaskInProgress(task: ScheduledTask, startedAt: DateTime)
}

class SchedulingActor(
    jobManager: JobManager[Future], 
    permissionsFinder: PermissionsFinder[Future], 
    vfs: VFS[Future],
    storage: ScheduleStorage[Future], 
    platform: Platform[Future, StreamT[Future, Slice]], 
    clock: Clock, 
    storageTimeout: Duration = Duration(30, TimeUnit.SECONDS), 
    resourceTimeout: Timeout = Timeout(10, TimeUnit.SECONDS)) extends Actor with Logging { 
  import SchedulingActor._
    
  private[this] final implicit val scheduleOrder: Ordering[(DateTime, ScheduledTask)] = Ordering.by(_._1.getMillis)

  private[this] implicit val M: Monad[Future] = new FutureMonad(context.dispatcher)

  private[this] implicit val executor = context.dispatcher

  // Although PriorityQueue is mutable, we're going to treat it as immutable since most methods on it act that way
  private[this] var scheduleQueue = PriorityQueue.empty[(DateTime, ScheduledTask)]

  private[this] var scheduledAwake: Option[Cancellable] = None

  private[this] var running = Map.empty[TaskKey, TaskInProgress]

  // We need to keep this around in case a task is running when its removal is requested
  private[this] var pendingRemovals = Set.empty[UUID]

  override def preStart = {
    val now = new Date

    storage.listTasks onSuccess { 
      case tasks => self ! AddTasksToQueue(tasks)
    }
  }

  override def postStop = {
    scheduledAwake foreach { sa =>
      if (! sa.isCancelled) {
        sa.cancel()
      }
    }
  }

  def scheduleNextTask(): Unit = {
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
    task.repeat.flatMap { sched => Option(sched.getNextValidTimeAfter(threshold)) } map { nextTime =>
      (new DateTime(nextTime), task)
    }
  }

  def rescheduleTasks(tasks: Seq[ScheduledTask]): Unit = {
    val (toRemove, toReschedule) = tasks.partition { task => pendingRemovals.contains(task.id) }

    toRemove foreach { task =>
      logger.info("Removing completed task after run: " + task.id)
      pendingRemovals -= task.id
    }

    scheduleQueue ++= {
      toReschedule flatMap { task =>
        nextRun(new Date, task) unsafeTap { next => 
          if (next.isEmpty) logger.warn("No further run times for " + task)
        }
      }
    }

    scheduleNextTask()
  }

  def removeTask(id: UUID) = {
    scheduleQueue = scheduleQueue.filterNot(_._2.id == id)
    pendingRemovals += id
  }

  def executeTask(task: ScheduledTask): Future[PrecogUnit] = {
    if (running.contains((task.source, task.sink))) {
      // We don't allow for more than one concurrent instance of a given task
      Promise successful PrecogUnit
    } else {
      def consumeStream(totalSize: Long, stream: StreamT[Future, Slice]): Future[Long] = {
        stream.uncons flatMap {
          case Some((x, xs)) => consumeStream(totalSize + x.size, xs)
          case None => M.point(totalSize)
        }
      }

      val ourself = self
      val startedAt = new DateTime

      implicit val readTimeout = resourceTimeout

      // This cannot occur inside a Future, or we would be exposing Actor state outside of this thread
      running += ((task.source, task.sink) -> TaskInProgress(task, startedAt))

      (for {
        job <- jobManager.createJob(task.apiKey, task.taskName, "scheduled", None, Some(startedAt))
        executorV <- platform.executorFor(task.apiKey)
        scriptV <- vfs.readQuery(task.source, Version.Current)
        execution <- (for {
          script <- scriptV.toSuccess("Could not find quirrel script at path %s".format(task.source.path))
          executor <- executorV
        } yield {
          permissionsFinder.writePermissions(task.apiKey, task.sink, clock.instant()) flatMap { perms =>
            val allPerms = Map(task.apiKey -> perms.toSet[Permission])

            executor.execute(task.apiKey, script, task.prefix, QueryOptions(timeout = task.timeout)).flatMap {
              _ traverse { stream =>
                consumeStream(0, vfs.persistingStream(task.apiKey, task.sink, sys.error("where is the authorities value???"), perms.toSet[Permission], Some(job.id), stream)) map { totalSize =>
                  ourself ! TaskComplete(task.id, clock.now(), totalSize, None)
                  totalSize
                }
              }
            }
          }
        }) valueOr { error =>
          jobManager.abort(job.id, error) map { _ =>
            ourself ! TaskComplete(task.id, clock.now(), 0, Some(error))
          }
        }
      } yield PrecogUnit) onFailure {
        case t: Throwable =>
          ourself ! TaskComplete(task.id, clock.now(), 0, Option(t.getMessage) orElse Some(t.getClass.toString))
          PrecogUnit
      }
    }
  }

  def receive = {
    case AddTask(repeat, apiKey, authorities, prefix, source, sink, timeout) =>
      val ourself = self
      val newTask = ScheduledTask(UUID.randomUUID(), repeat, apiKey, authorities, prefix, source, sink, timeout)
      (repeat match {
        case None =>
          executeTask(newTask)

        case Some(_) =>
          storage.addTask(newTask) map { addV =>
            addV foreach { task => ourself ! AddTasksToQueue(Seq(task)) }
            addV map (_.id)
          }
      }) recover {
        case t: Throwable =>
          logger.error("Error adding task " + newTask, t)
          Failure("Internal error adding task")
      } pipeTo sender

    case DeleteTask(id) =>
      val ourself = self
      storage.deleteTask(id) map { deleteV =>
        deleteV foreach { _ =>
          ourself ! RemoveTaskFromQueue(id)
        }
        deleteV
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

    case AddTasksToQueue(tasks) =>
      rescheduleTasks(tasks)

    case RemoveTaskFromQueue(id) =>
      removeTask(id)

    case WakeForRun =>
      val now = new DateTime
      val (toRun, newQueue) = scheduleQueue.partition(_._1.isAfter(now))
      scheduleQueue = newQueue
      toRun.map(_._2).foreach(executeTask)

      scheduleNextTask()

    case TaskComplete(id, endedAt, total, error) =>
      running.values.find(_.task.id == id) match {
        case Some(TaskInProgress(task, startAt)) =>
          error match {
            case None =>
              logger.info("Scheduled task %s completed with %d records in %d millis".format(id, total, (new JodaDuration(startAt, endedAt)).getMillis))

            case Some(error) =>
              logger.warn("Scheduled task %s failed after %d millis: %s".format(id, (new JodaDuration(startAt, clock.now())).getMillis, error))
          }

          storage.reportRun(ScheduledRunReport(id, startAt, endedAt, total, error.toList))
          running -= (task.source -> task.sink)
          rescheduleTasks(Seq(task))

        case None =>
          logger.error("Task completion reported for unknown task " + id)
      }
  }
}
