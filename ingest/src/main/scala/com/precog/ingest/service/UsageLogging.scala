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
package com.precog.ingest
package service

import blueeyes._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.persistence.cache._

import java.util.concurrent.TimeUnit
import scalaz.Semigroup

import com.precog.common._

trait UsageLogging {
  def apiKey: String
  def tracked(path: Path, count: Int)
  def tracked(path: Path, count: Int, complexity: Long)
}

class NullUsageLogging(val apiKey: String) extends UsageLogging {
  def tracked(path: Path, count: Int) = Unit
  def tracked(path: Path, count: Int, complexity: Long) = Unit
}

class ReportGridUsageLogging(val apiKey: String) extends UsageLogging {
  def expirationPolicy = ExpirationPolicy(
    timeToIdle = Some(30), 
    timeToLive = Some(120),
    timeUnit = TimeUnit.SECONDS
  )

  val stage = Stage[Path, StorageMetrics](expirationPolicy, 0) { 
    case (path, StorageMetrics(count, complexity)) =>
 //     client.track(
 //       Trackable(
 //         path = path.toString,
 //         name = "stored",
 //         properties = JObject(JField("#timestamp", "auto") :: JField("count", count) :: JField("complexity", complexity) :: Nil),
 //         rollup = true
 //       )
 //     )
  }

  def tracked(path: Path, count: Int) = {
    stage.put(path, StorageMetrics(count, None))
  }
  
  def tracked(path: Path, count: Int, complexity: Long) = {
    stage.put(path, StorageMetrics(count, Some(complexity)))
  }
}

case class StorageMetrics(count: Int, complexity: Option[Long])
object StorageMetrics {
  implicit val Semigroup: Semigroup[StorageMetrics] = new Semigroup[StorageMetrics] {
    override def append(s1: StorageMetrics, s2: => StorageMetrics) = {
      val count = s1.count + s2.count
      val complexity = (s1.complexity, s2.complexity) match {
        case (None, None)         => None
        case (Some(c), None)      => Some(c)
        case (None, Some(c))      => Some(c)
        case (Some(c1), Some(c2)) => Some(c1 + c2)
      }
      StorageMetrics(count, complexity)
    }
  }
}
