package com.precog.yggdrasil
package scheduling

import com.precog.common.Path
import com.precog.common.security._
import com.precog.util.PrecogUnit

import java.util.UUID

import org.quartz.CronExpression

import scalaz.EitherT

trait ScheduleStorage[M[+_]] {
  def addTask(task: ScheduledTask): EitherT[M, String, ScheduledTask]

  def deleteTask(id: UUID): EitherT[M, String, Option[ScheduledTask]]

  def reportRun(report: ScheduledRunReport): M[PrecogUnit]

  def statusFor(id: UUID, lastLimit: Option[Int]): M[Option[(ScheduledTask, Seq[ScheduledRunReport])]]

  def listTasks: M[Seq[ScheduledTask]]
}
