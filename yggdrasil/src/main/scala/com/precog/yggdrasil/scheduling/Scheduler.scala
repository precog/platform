package com.precog.yggdrasil
package scheduling

import akka.actor.ActorRef
import akka.dispatch.Future
import akka.pattern.ask
import akka.util.Timeout

import com.precog.common.Path
import com.precog.common.security._
import com.precog.util.PrecogUnit
import com.precog.yggdrasil.execution.EvaluationContext

import java.util.UUID

import org.quartz.CronExpression

import scalaz._

trait Scheduler[M[+_]] {
  def enabled: Boolean

  def addTask(repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, context: EvaluationContext, source: Path, sink: Path, timeoutMillis: Option[Long]): EitherT[M, String, UUID]

  def deleteTask(id: UUID): EitherT[M, String, PrecogUnit]

  def statusForTask(id: UUID, limit: Option[Int]): EitherT[M, String, Option[(ScheduledTask, Seq[ScheduledRunReport])]]
}

class ActorScheduler(scheduler: ActorRef, timeout: Timeout) extends Scheduler[Future] {
  implicit val requestTimeout = timeout
  val enabled = true

  def addTask(repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, context: EvaluationContext, source: Path, sink: Path, timeoutMillis: Option[Long]): EitherT[Future, String, UUID] = EitherT {
    (scheduler ? AddTask(repeat, apiKey, authorities, context, source, sink, timeoutMillis)).mapTo[String \/ UUID]
  }

  def deleteTask(id: UUID) = EitherT {
    (scheduler ? DeleteTask(id)).mapTo[String \/ PrecogUnit]
  }

  def statusForTask(id: UUID, limit: Option[Int]) = EitherT {
    (scheduler ? StatusForTask(id, limit)).mapTo[String \/ Option[(ScheduledTask, Seq[ScheduledRunReport])]]
  }
}

object NoopScheduler {
  def apply[M[+_]: Monad] = new NoopScheduler[M]
}

class NoopScheduler[M[+_]](implicit M: Monad[M]) extends Scheduler[M] {
  val enabled = false

  def addTask(repeat: Option[CronExpression], apiKey: APIKey, authorities: Authorities, context: EvaluationContext, source: Path, sink: Path, timeoutMillis: Option[Long]) = sys.error("No scheduling available")

  def deleteTask(id: UUID) = sys.error("No scheduling available")

  def statusForTask(id: UUID, limit: Option[Int]) = sys.error("No scheduling available")
}
