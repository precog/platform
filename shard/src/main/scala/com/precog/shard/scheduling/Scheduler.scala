package com.precog.shard
package scheduling

import akka.actor.ActorRef
import akka.dispatch.Future
import akka.pattern.ask
import akka.util.Timeout

import com.precog.util.PrecogUnit

import java.util.UUID

import scalaz._

trait Scheduler[M[_]] {
  def enabled: Boolean

  def addTask(task: ScheduledTask): M[Validation[String, PrecogUnit]]

  def deleteTask(id: UUID): M[Validation[String, PrecogUnit]]

  def statusForTask(id: UUID, limit: Option[Int]): M[Validation[String, Option[(ScheduledTask, Seq[ScheduledRunReport])]]]
}

class ActorScheduler(scheduler: ActorRef)(implicit timeout: Timeout) extends Scheduler[Future] {
  val enabled = true

  def addTask(task: ScheduledTask) =
    (scheduler ? AddTask(task)).mapTo[Validation[String, PrecogUnit]]

  def deleteTask(id: UUID) =
    (scheduler ? DeleteTask(id)).mapTo[Validation[String, PrecogUnit]]

  def statusForTask(id: UUID, limit: Option[Int]) =
    (scheduler ? StatusForTask(id, limit)).mapTo[Validation[String, Option[(ScheduledTask, Seq[ScheduledRunReport])]]]
}

object NoopScheduler extends Scheduler[Future] {
  val enabled = false

  def addTask(task: ScheduledTask) = sys.error("No scheduling available")

  def deleteTask(id: UUID) = sys.error("No scheduling available")

  def statusForTask(id: UUID, limit: Option[Int]) = sys.error("No scheduling available")
}
