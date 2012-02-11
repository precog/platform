package com.precog.ingest
package service

import blueeyes._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.persistence.cache._

import IngestService._
//import com.precog.api.ReportGridTrackingClient
//import com.precog.api.Trackable
//import rosetta.json.blueeyes._

import java.util.concurrent.TimeUnit
import scalaz.Semigroup

import com.precog.analytics._
import com.precog.common._

trait UsageLogging {
  def tokenId: String
  def tracked(path: Path, count: Int)
  def tracked(path: Path, count: Int, complexity: Long)
}

class NullUsageLogging(val tokenId: String) extends UsageLogging {
  def tracked(path: Path, count: Int) = Unit
  def tracked(path: Path, count: Int, complexity: Long) = Unit
}

class ReportGridUsageLogging(val tokenId: String) extends UsageLogging {
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
