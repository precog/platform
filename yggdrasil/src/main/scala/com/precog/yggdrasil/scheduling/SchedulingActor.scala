package com.precog.yggdrasil
package scheduling

import com.precog.common.Path
import com.precog.common.accounts.AccountFinder
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.util.PrecogUnit
import com.precog.yggdrasil.execution._
import com.precog.yggdrasil.table.Slice
import com.precog.yggdrasil.vfs._

import akka.actor.{Actor, ActorRef, ActorSystem, Cancellable}
import akka.dispatch.{Await, Future, Promise}
import akka.pattern.{ask, pipe}
import akka.util.{Duration, Timeout}

import blueeyes.bkka.FutureMonad
import blueeyes.util.Clock

import com.weiglewilczek.slf4s.Logging

import java.util.{Date, UUID}
import java.util.concurrent.TimeUnit

import org.joda.time.{Duration => JodaDuration, DateTime}

import org.quartz.CronExpression

import scala.collection.mutable.{ArrayBuffer, PriorityQueue}

import scalaz.{Ordering => _, idInstance => _, _}
import scalaz.std.option._
import scalaz.syntax.id._
import scalaz.syntax.traverse._
import scalaz.syntax.std.either._
import scalaz.syntax.std.option._
import scalaz.effect.IO

sealed trait SchedulingMessage

case class AddTask(repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, prefix: Path, source: Path, sink: Path, timeoutMillis: Option[Long])

case class DeleteTask(id: UUID)

case class StatusForTask(id: UUID, limit: Option[Int])

trait SchedulingActorModule extends SecureVFSModule[Future, Slice] {
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
      storage: ScheduleStorage[Future], 
      platform: Platform[Future, Slice, StreamT[Future, Slice]], 
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
      import EvaluationError._

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

        
        val execution = for {
          basePath <- EitherT(M point { task.source.prefix \/> invalidState("Path %s cannot be relativized.".format(task.source.path)) })
          cachingResult <- platform.vfs.executeAndCache(platform, task.apiKey, task.source, basePath, QueryOptions(timeout = task.timeout), Some(task.sink), Some(task.taskName))


        } yield cachingResult
        
        execution.fold[Future[PrecogUnit]](
          failure => M point {
            logger.error("An error was encountered processing a scheduled query execution: " + failure)
            ourself ! TaskComplete(task.id, clock.now(), 0, Some(failure.toString)) : PrecogUnit
          },
          storedQueryResult => {
            consumeStream(0, storedQueryResult.data) map { totalSize =>
              ourself ! TaskComplete(task.id, clock.now(), totalSize, None)
              PrecogUnit
            } recoverWith {
              case t: Throwable =>
                for {
                  _ <- storedQueryResult.cachingJob.traverse { jobId =>
                    jobManager.abort(jobId, t.getMessage) map {
                      case Right(jobAbortSuccess) => 
                          ourself ! TaskComplete(task.id, clock.now(), 0, Option(t.getMessage) orElse Some(t.getClass.toString))   
                      case Left(jobAbortFailure) => sys.error(jobAbortFailure.toString)
                    }
                  } 
                } yield PrecogUnit
            }
          }
        ) flatMap {
          identity[Future[PrecogUnit]]
        } onFailure {
          case t: Throwable =>
            logger.error("Scheduled query execution failed by thrown error.", t)
            ourself ! TaskComplete(task.id, clock.now(), 0, Option(t.getMessage) orElse Some(t.getClass.toString)) : PrecogUnit
        }
      }
    }

    def receive = {
      case AddTask(repeat, apiKey, authorities, prefix, source, sink, timeout) =>
        val ourself = self
        val taskId = UUID.randomUUID()
        val newTask = ScheduledTask(taskId, repeat, apiKey, authorities, prefix, source, sink, timeout)
        val addResult: EitherT[Future, String, PrecogUnit] = repeat match {
          case None =>
            EitherT.right(executeTask(newTask))

          case Some(_) =>
            storage.addTask(newTask) map { task =>
              ourself ! AddTasksToQueue(Seq(task)) 
            }
        }
        
        addResult.run.map(_ => taskId) recover {
          case t: Throwable =>
            logger.error("Error adding task " + newTask, t)
            \/.left("Internal error adding task")
        } pipeTo sender

      case DeleteTask(id) =>
        val ourself = self
        val deleteResult = storage.deleteTask(id) map { result =>
          ourself ! RemoveTaskFromQueue(id)
          result
        } 
        
        deleteResult.run recover {
          case t: Throwable =>
            logger.error("Error deleting task " + id, t)
            \/.left("Internal error deleting task")
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
}
