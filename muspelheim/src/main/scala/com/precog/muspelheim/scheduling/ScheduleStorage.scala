package com.precog.muspelheim
package scheduling

import com.precog.common.Path
import com.precog.common.security._
import com.precog.util.PrecogUnit

import java.util.UUID

import org.quartz.CronExpression

import scalaz.Validation

trait ScheduleStorage[M[+_]] {
  def addTask(task: ScheduledTask): M[Validation[String, ScheduledTask]]

  def deleteTask(id: UUID): M[Validation[String, Option[ScheduledTask]]]

  def reportRun(report: ScheduledRunReport): M[PrecogUnit]

  def statusFor(id: UUID, lastLimit: Option[Int]): M[Option[(ScheduledTask, Seq[ScheduledRunReport])]]

  def listTasks: M[Seq[ScheduledTask]]
}
