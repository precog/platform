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
import blueeyes.util.Clock

import org.slf4j.{ LoggerFactory, Logger }

import scalaz._
import scalaz.syntax.monad._

trait QueryLogger[M[+_]] {

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
trait JobQueryLogger[M[+_]] extends QueryLogger[M] {
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

trait LoggingQueryLogger[M[+_]] extends QueryLogger[M] {
  implicit def M: Applicative[M]

  protected val logger = LoggerFactory.getLogger("com.precog.daze.QueryLogger")

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

object LoggingQueryLogger {
  def apply[M[+_]](implicit M0: Applicative[M]): QueryLogger[M] = {
    new LoggingQueryLogger[M] {
      val M = M0
    }
  }
}
