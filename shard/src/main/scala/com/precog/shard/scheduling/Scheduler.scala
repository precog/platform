package com.precog.shard
package scheduling

import akka.actor.ActorRef
import akka.dispatch.Future
import akka.pattern.ask
import akka.util.Timeout

import com.precog.common.Path
import com.precog.common.security._
import com.precog.util.PrecogUnit
import com.precog.daze.EvaluationContext

import java.util.UUID

import org.quartz.CronExpression

import scalaz._

trait Scheduler[M[_]] {
  def enabled: Boolean

  def addTask(repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, context: EvaluationContext, source: Path, sink: Path, timeoutMillis: Option[Long]): M[Validation[String, UUID]]

  def deleteTask(id: UUID): M[Validation[String, PrecogUnit]]

  def statusForTask(id: UUID, limit: Option[Int]): M[Validation[String, Option[(ScheduledTask, Seq[ScheduledRunReport])]]]
}

class ActorScheduler(scheduler: ActorRef, timeout: Timeout) extends Scheduler[Future] {
  implicit val requestTimeout = timeout
  val enabled = true

  def addTask(repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, context: EvaluationContext, source: Path, sink: Path, timeoutMillis: Option[Long]): Future[Validation[String, UUID]] =
    (scheduler ? AddTask(repeat, apiKey, authorities, context, source, sink, timeoutMillis)).mapTo[Validation[String, UUID]]

  def deleteTask(id: UUID) =
    (scheduler ? DeleteTask(id)).mapTo[Validation[String, PrecogUnit]]

  def statusForTask(id: UUID, limit: Option[Int]) =
    (scheduler ? StatusForTask(id, limit)).mapTo[Validation[String, Option[(ScheduledTask, Seq[ScheduledRunReport])]]]
}

object NoopScheduler {
  def apply[M[+_]: Monad] = new NoopScheduler[M]
}

class NoopScheduler[M[+_]](implicit M: Monad[M]) extends Scheduler[M] {
  val enabled = false

  def addTask(repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, context: EvaluationContext, source: Path, sink: Path, timeoutMillis: Option[Long]): M[Validation[String, UUID]] = sys.error("No scheduling available")

  def deleteTask(id: UUID) = sys.error("No scheduling available")

  def statusForTask(id: UUID, limit: Option[Int]) = sys.error("No scheduling available")
}
