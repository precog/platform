package com.precog.yggdrasil
package scheduling

import akka.dispatch.{ExecutionContext, Future, Promise}

import com.precog.util.PrecogUnit

import java.util.UUID

import scalaz.Success

class InMemoryScheduleStorage(implicit executor: ExecutionContext) extends ScheduleStorage[Future] {
  private[this] var tasks = Map.empty[UUID, ScheduledTask]
  private[this] var history = Map.empty[UUID, Seq[ScheduledRunReport]]

  def addTask(task: ScheduledTask) = Promise successful {
    tasks += (task.id -> task)
    Success(task)
  }

  def deleteTask(id: UUID) = Promise successful {
    val found = tasks.get(id)
    tasks -= id
    Success(found)
  }

  def reportRun(report: ScheduledRunReport) = Promise successful {
    history += (report.id -> (history.getOrElse(report.id, Seq.empty[ScheduledRunReport]) :+ report))
    PrecogUnit
  }

  def statusFor(id: UUID, limit: Option[Int]) = Promise successful {
    tasks.get(id) map { task =>
      val reports = history.getOrElse(id, Seq.empty[ScheduledRunReport])
      (task, limit map(reports.take) getOrElse reports)
    }
  }

  def listTasks = Promise successful {
    tasks.values.toSeq
  }
}
