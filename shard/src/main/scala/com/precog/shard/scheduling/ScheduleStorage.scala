package com.precog.shard
package scheduling

import com.precog.util.PrecogUnit

import java.util.UUID

import scalaz.Validation

trait ScheduleStorage[M[+_]] {
  def addTask(task: ScheduledTask): M[Validation[String, PrecogUnit]]

  def deleteTask(id: UUID): M[Validation[String, Option[ScheduledTask]]]

  def reportRun(report: ScheduledRunReport): M[PrecogUnit]

  def statusFor(id: UUID, lastLimit: Option[Int]): M[Option[(ScheduledTask, Seq[ScheduledRunReport])]]

  def listTasks: M[Seq[ScheduledTask]]
}
