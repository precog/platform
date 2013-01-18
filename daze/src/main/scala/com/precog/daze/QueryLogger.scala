/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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

trait QueryLogger[M[+_], -P] {

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
trait JobQueryLogger[M[+_], -P] extends QueryLogger[M, P] {
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

trait LoggingQueryLogger[M[+_], -P] extends QueryLogger[M, P] {
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
  def apply[M[+_]](implicit M0: Applicative[M]): QueryLogger[M, Any] = {
    new LoggingQueryLogger[M, Any] {
      val M = M0
    }
  }
}

trait ExceptionQueryLogger[M[+_], -P] extends QueryLogger[M, P] {
  implicit def M: Applicative[M]
  
  abstract override def fatal(pos: P, msg: String): M[Unit] = for {
    _ <- super.fatal(pos, msg)
    _ = throw FatalQueryException(pos, msg)
  } yield ()
}

case class FatalQueryException[+P](pos: P, msg: String) extends RuntimeException(msg)
