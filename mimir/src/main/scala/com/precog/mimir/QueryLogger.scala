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
package com.precog.mimir

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

import scala.collection.JavaConverters._
import scala.annotation.tailrec

import java.util.concurrent.ConcurrentHashMap

trait QueryLogger[M[+_], -P] { self =>
  def contramap[P0](f: P0 => P): QueryLogger[M, P0] = new QueryLogger[M, P0] {
    def die(): M[Unit] = self.die()
    def error(pos: P0, msg: String): M[Unit] = self.error(f(pos), msg)
    def warn(pos: P0, msg: String): M[Unit] = self.warn(f(pos), msg)
    def info(pos: P0, msg: String): M[Unit] = self.info(f(pos), msg)
    def log(pos: P0, msg: String): M[Unit] = self.log(f(pos), msg)
    def timing(pos: P0, nanos: Long): M[Unit] = self.timing(f(pos), nanos)
    def done: M[Unit] = self.done
  }

  def die(): M[Unit]

  /**
   * This reports a error to the user. Depending on the implementation, this may
   * also stop computation completely.
   */
  def error(pos: P, msg: String): M[Unit]

  /**
   * Report a warning to the user.
   */
  def warn(pos: P, msg: String): M[Unit]

  /**
   * Report an informational message to the user.
   */
  def info(pos: P, msg: String): M[Unit]
  
  /**
   * Report an information message for internal use only
   */
  def log(pos: P, msg: String): M[Unit]
  
  /**
   * Record timing information for a particular position.  Note that a position
   * may record multiple timing events, which should be aggregated according to
   * simple summary statistics.
   *
   * Please note the following:
   *
   * kx = 303 seconds 
   *   where
   *     2^63 - 1 = sum i from 0 to k, x^2
   *     x > 0
   *     k = 10000    (an arbitrary, plausible iteration count)
   * 
   * This is to say that, for a particular position which is hit 10,000 times,
   * the total time spent in that particular position must be bounded by 303
   * seconds to avoid signed Long value overflow.  Conveniently, our query timeout
   * is 300 seconds, so this is not an issue.
   */
  def timing(pos: P, nanos: Long): M[Unit]
  
  def done: M[Unit]
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

  def die(): M[Unit] = for {
    _ <- jobManager.cancel(jobId, "Cancelled because of error.", clock.now())
  } yield ()

  def error(pos: P, msg: String): M[Unit] = send(channels.Error, pos, msg)

  def warn(pos: P, msg: String): M[Unit] = send(channels.Warning, pos, msg)

  def info(pos: P, msg: String): M[Unit] = send(channels.Info, pos, msg)
  
  def log(pos: P, msg: String): M[Unit] = send(channels.Log, pos, msg)
}

trait LoggingQueryLogger[M[+_], P] extends QueryLogger[M, P] {
  implicit def M: Applicative[M]

  protected val logger = LoggerFactory.getLogger("com.precog.mimir.QueryLogger")

  def die(): M[Unit] = M.point { () }

  def error(pos: P, msg: String): M[Unit] = M.point {
    logger.error(pos.toString + " - " + msg)
  }

  def warn(pos: P, msg: String): M[Unit] = M.point {
    logger.warn(pos.toString + " - " + msg)
  }

  def info(pos: P, msg: String): M[Unit] = M.point {
    logger.info(pos.toString + " - " + msg)
  }

  def log(pos: P, msg: String): M[Unit] = info(pos, msg)
}

object LoggingQueryLogger {
  def apply[M[+_]](implicit M0: Monad[M]): QueryLogger[M, Any] = {
    new LoggingQueryLogger[M, Any] with TimingQueryLogger[M, Any] {
      val M = M0
    }
  }
}

trait TimingQueryLogger[M[+_], P] extends QueryLogger[M, P] {
  implicit def M: Monad[M]
  
  private val table = new ConcurrentHashMap[P, Stats]

  def timing(pos: P, nanos: Long): M[Unit] = {
    @tailrec
    def loop() {
      val stats = table get pos
      
      if (stats == null) {
        val stats = Stats(1, nanos, nanos * nanos, nanos, nanos)
        
        if (table.putIfAbsent(pos, stats) != stats) {
          loop()
        }
      } else {
        if (!table.replace(pos, stats, stats derive nanos)) {
          loop()
        }
      }
    }
    
    M point {
      loop()
    }
  }
  
  def done: M[Unit] = {
    val logging = table.asScala map {
      case (pos, stats) =>
        log(pos, """{"count":%d,"sum":%d,"sumSq":%d,"min":%d,"max":%d}""".format(stats.count, stats.sum, stats.sumSq, stats.min, stats.max))
    }
    
    logging reduceOption { _ >> _ } getOrElse (M point ())
  }
  
  private case class Stats(count: Long, sum: Long, sumSq: Long, min: Long, max: Long) {
    final def derive(nanos: Long): Stats = {
      copy(
        count = count + 1,
        sum = sum + nanos,
        sumSq = sumSq + (nanos * nanos),
        min = min min nanos,
        max = max max nanos)
    }
  }
}

trait ExceptionQueryLogger[M[+_], -P] extends QueryLogger[M, P] {
  implicit def M: Applicative[M]
  
  abstract override def die(): M[Unit] = for {
    _ <- super.die()
    _ = throw FatalQueryException("Query terminated abnormally.")
  } yield ()
}

case class FatalQueryException(msg: String) extends RuntimeException(msg)
