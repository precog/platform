package com.precog.shard
package scheduling

import com.precog.util.PrecogUnit

import java.util.UUID

trait ScheduleStorage[M[+_]] {
  def addTask(task: ScheduledTask): M[PrecogUnit]

  def deleteTask(id: UUID): M[Option[ScheduledTask]]

  def reportRun(report: ScheduledRunReport): M[PrecogUnit]

  def historyFor(id: UUID): M[Seq[ScheduledRunReport]]

  def listTasks: M[Seq[ScheduledTask]]
}
