package com.precog.shard
package scheduling

import com.precog.util.PrecogUnit

import java.util.UUID

import org.joda.time.DateTime

case class ScheduledRunReport(id: UUID, startedAt: DateTime, endedAt: DateTime, records: Long, messages: Seq[String] = Seq.empty)

trait ScheduleStorage[M[+_]] {
  def addTask(task: ScheduledTask): M[PrecogUnit]

  def deleteTask(id: UUID): M[PrecogUnit]

  def reportRun(report: ScheduledRunReport): M[PrecogUnit]

  def historyFor(id: UUID): M[Seq[ScheduledRunReport]]

  def listTasks: M[Seq[ScheduledTask]]
}
