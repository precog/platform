package com.precog.daze

import com.precog.common.jobs._
import com.precog.common.security._

import blueeyes.json._
import blueeyes.json.{ serialization => _, _ }
import blueeyes.json.serialization.SerializationImplicits._
import blueeyes.json.serialization._

import blueeyes.util.Clock

import org.slf4j.{ LoggerFactory, Logger }

import scalaz._
import scalaz.syntax.monad._

trait QueryLogger[M[+_], P] { self =>
  def contramap[P0](f: P0 => P): QueryLogger[M, P0] = new QueryLogger[M, P0] {
    def fatal(pos: P0, msg: String): M[Unit] = self.fatal(f(pos), msg)
    def warn(pos: P0, msg: String): M[Unit] = self.warn(f(pos), msg)
    def info(pos: P0, msg: String): M[Unit] = self.info(f(pos), msg)
  }

  /**
   * This reports a fatal error user. Depending on the implementation, this may
   * also stop computation completely.
   */
  def fatal(pos: P, msg: String): M[Unit]

  /**
   * Report a warning to the user.
   */
  def warn(pos: P, msg: String): M[Unit]

  /**
   * Report an informational message to the user.
   */
  def info(pos: P, msg: String): M[Unit]
}

/**
 * Reports errors to a job's channel.
 */
trait JobQueryLogger[M[+_], P] extends QueryLogger[M, P] {
  import JobManager._

  implicit def M: Monad[M]

  def jobManager: JobManager[M]
  def jobId: JobId
  def clock: Clock

  protected def decomposer: Decomposer[P]

  protected def mkMessage(pos: P, msg: String): JValue = {
    JObject(
      JField("message", JString(msg)) ::
      JField("timestamp", clock.now().serialize) ::
      JField("position", decomposer.decompose(pos)) ::
      Nil)
  }

  private def send(channel: String, pos: P, msg: String): M[Unit] =
    jobManager.addMessage(jobId, channel, mkMessage(pos, msg)) map { _ => () }

  def fatal(pos: P, msg: String): M[Unit] = for {
    _ <- send(channels.Error, pos, msg)
    _ <- jobManager.cancel(jobId, "Cancelled because of error: " + msg, clock.now())
  } yield ()

  def warn(pos: P, msg: String): M[Unit] = send(channels.Warning, pos, msg)

  def info(pos: P, msg: String): M[Unit] = send(channels.Info, pos, msg)
}

trait LoggingQueryLogger[M[+_], P] extends QueryLogger[M, P] {
  implicit def M: Applicative[M]

  protected val logger = LoggerFactory.getLogger("com.precog.daze.QueryLogger")

  def fatal(pos: P, msg: String): M[Unit] = M.point {
    logger.error(msg)
  }

  def warn(pos: P, msg: String): M[Unit] = M.point {
    logger.warn(msg)
  }

  def info(pos: P, msg: String): M[Unit] = M.point {
    logger.info(msg)
  }
}

object LoggingQueryLogger {
  def apply[M[+_], P](implicit M0: Applicative[M]): QueryLogger[M, P] = {
    new LoggingQueryLogger[M, P] {
      val M = M0
    }
  }
}

trait ExceptionQueryLogger[M[+_], P] extends QueryLogger[M, P] {
  implicit def M: Applicative[M]
  
  abstract override def fatal(pos: P, msg: String): M[Unit] = for {
    _ <- super.fatal(pos, msg)
    _ = throw FatalQueryException(pos, msg)
  } yield ()
}

case class FatalQueryException[+P](pos: P, msg: String) extends RuntimeException(msg)
