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

import kafka._

import com.precog.daze._
import com.precog.common.Path
import com.precog.common.security._

import org.specs2.mutable.Specification
import org.specs2.specification._
import org.scalacheck.Gen._

import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext
import akka.util.Duration

import org.joda.time._

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import scalaz.{Success, NonEmptyList}
import scalaz.Scalaz._

import blueeyes.concurrent.test._

import blueeyes.core.data._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._

import blueeyes.json.JsonAST._

import blueeyes.util.Clock


case class PastClock(duration: org.joda.time.Duration) extends Clock {
  def now() = new DateTime().minus(duration)
  def instant() = now().toInstant
  def nanoTime = sys.error("nanotime not available in the past")
}

trait TestTokens {
  import StaticTokenManager._
  val TestToken = lookup(testUID).get
  val TrackingToken = lookup(usageUID).get
}

trait TestIngestService extends BlueEyesServiceSpecification with IngestService with LocalMongo with TestTokens {

  import BijectionsChunkJson._

  val requestLoggingData = """
    requestLog {
      enabled = true
      fields = "time cs-method cs-uri sc-status cs-content"
    }
  """

  override val configuration = "services { ingest { v1 { " + requestLoggingData + mongoConfigFileData + " } } }"


  def tokenManagerFactory(config: Configuration) = StaticTokenManager 
  
  def usageLoggingFactory(config: Configuration) = new ReportGridUsageLogging(TrackingToken.uid) 

  val messaging = new CollectingMessaging

  def queryExecutorFactory(config: Configuration) = new NullQueryExecutor {
    lazy val actorSystem = ActorSystem("ingest_service_spec")
    implicit lazy val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  }

  def eventStoreFactory(config: Configuration): EventStore = {
    val defaultAddresses = NonEmptyList(MailboxAddress(0))

    val routeTable = new ConstantRouteTable(defaultAddresses)

    new KafkaEventStore(new EventRouter(routeTable, messaging), 0)
  }

  lazy val jsonTestService = service.contentType[JValue](application/(MimeTypes.json)).
                                     query("tokenId", TestToken.uid)

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(20, Duration(1, "second"))
  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))
}

class IngestServiceSpec extends TestIngestService with FutureMatchers {
  "Ingest Service" should {
    "abc123" must_== "abc123"
  }
}

trait LocalMongo extends Specification {
  val eventsName = "testev" + scala.util.Random.nextInt(10000)
  val indexName =  "testix" + scala.util.Random.nextInt(10000)

  def mongoConfigFileData = """
    eventsdb {
      database = "%s"
      servers  = ["127.0.0.1:27017"]
    }

    indexdb {
      database = "%s"
      servers  = ["127.0.0.1:27017"]
    }

    tokens {
      collection = "tokens"
    }

    variable_series {
      collection = "variable_series"
      time_to_idle_millis = 100
      time_to_live_millis = 100

      initial_capacity = 100
      maximum_capacity = 100
    }

    variable_value_series {
      collection = "variable_value_series"

      time_to_idle_millis = 100
      time_to_live_millis = 100

      initial_capacity = 100
      maximum_capacity = 100
    }

    variable_values {
      collection = "variable_values"

      time_to_idle_millis = 100
      time_to_live_millis = 100

      initial_capacity = 100
      maximum_capacity = 100
    }

    variable_children {
      collection = "variable_children"

      time_to_idle_millis = 100
      time_to_live_millis = 100

      initial_capacity = 100
      maximum_capacity = 100
    }

    path_children {
      collection = "path_children"

      time_to_idle_millis = 100
      time_to_live_millis = 100

      initial_capacity = 100
      maximum_capacity = 100
    }

    log {
      level   = "warning"
      console = true
    }
  """.format(eventsName, indexName)

  // We need to remove the databases used from Mongo after we're done
  def cleanupDb = Step {
    try {
      val conn = new com.mongodb.Mongo("localhost")

      conn.getDB(eventsName).dropDatabase()
      conn.getDB(indexName).dropDatabase()

      conn.close()
    } catch {
      case t => println("Error on DB cleanup: " + t.getMessage)
    }
  }

  override def map(fs : => Fragments) = super.map(fs) ^ cleanupDb

}
