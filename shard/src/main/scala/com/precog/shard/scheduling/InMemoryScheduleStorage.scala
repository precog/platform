package com.precog.shard
package scheduling

import akka.dispatch.{ExecutionContext, Future, Promise}

import com.precog.util.PrecogUnit

import java.util.UUID

class InMemoryScheduleStorage(implicit executor: ExecutionContext) extends ScheduleStorage[Future] {
  private[this] var tasks = Map.empty[UUID, ScheduledTask]
  private[this] var history = Map.empty[UUID, Seq[ScheduledRunReport]]

  def addTask(task: ScheduledTask) = Promise successful {
    tasks += (task.id -> task)
    PrecogUnit
  }

  def deleteTask(id: UUID) = Promise successful {
    val found = tasks.get(id)
    tasks -= id
    found
  }

  def reportRun(report: ScheduledRunReport) = Promise successful {
    history += (report.id -> (history.getOrElse(report.id, Seq.empty[ScheduledRunReport]) :+ report))
    PrecogUnit
  }

  def historyFor(id: UUID) = Promise successful {
    history.getOrElse(id, Seq.empty[ScheduledRunReport])
  }

  def listTasks = Promise successful {
    tasks.values.toSeq
  }
}
