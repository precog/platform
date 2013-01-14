package com.precog.daze

import com.precog.common.jobs._
import com.precog.common.security._

import blueeyes.json._
import blueeyes.json.{ serialization => _, _ }
import blueeyes.json.serialization.SerializationImplicits._
import blueeyes.util.Clock

import org.slf4j.LoggerFactory

import scalaz._
import scalaz.syntax.monad._

trait ErrorReport[M[+_]] {

  /**
   * This reports a fatal error user. Depending on the implementation, this may
   * also stop computation completely.
   */
  def fatal(msg: => String): M[Unit]

  /**
   * Report a warning to the user.
   */
  def warn(msg: => String): M[Unit]

  /**
   * Report an informational message to the user.
   */
  def info(msg: => String): M[Unit]
}

/**
 * Reports errors to a job's channel.
 */
trait JobErrorReport[M[+_]] extends ErrorReport[M] {
  import JobManager._

  implicit def M: Monad[M]

  def jobManager: JobManager[M]
  def jobId: JobId
  def clock: Clock

  private def mkMessage(msg: String): JValue = {
    JObject(
      JField("message", JString(msg)) ::
      JField("timestamp", clock.now().serialize) ::
      Nil)
  }

  private def send(channel: String, msg: String): M[Unit] =
    jobManager.addMessage(jobId, channel, mkMessage(msg)) map { _ => () }

  def fatal(msg: => String): M[Unit] = for {
    _ <- send(channels.Error, msg)
    _ <- jobManager.cancel(jobId, "Cancelled because of error: " + msg, clock.now())
  } yield ()

  def warn(msg: => String): M[Unit] = send(channels.Warning, msg)

  def info(msg: => String): M[Unit] = send(channels.Info, msg)
}

final class LoggingErrorReport[M[+_]](implicit M: Applicative[M]) extends ErrorReport[M] {
  private val logger = LoggerFactory.getLogger("com.precog.daze.ErrorReport")

  def fatal(msg: => String): M[Unit] = M.point {
    logger.error(msg)
  }

  def warn(msg: => String): M[Unit] = M.point {
    logger.warn(msg)
  }

  def info(msg: => String): M[Unit] = M.point {
    logger.info(msg)
  }
}
