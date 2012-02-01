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
import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.concurrent.test._
import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JsonDSL._
import blueeyes.json.xschema.JodaSerializationImplicits._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.JPathImplicits._
import blueeyes.persistence.mongo.{Mongo, RealMongo, MockMongo, MongoCollection, Database}
import blueeyes.util.metrics.Duration._
import blueeyes.util.Clock
import MimeTypes._

import akka.dispatch.Future
import akka.dispatch.Await
import akka.util.Duration

import org.joda.time._
import net.lag.configgy.ConfigMap

import org.specs2.mutable.Specification
import org.specs2.specification._
import org.scalacheck.Gen._
import scalaz.{Success, NonEmptyList}
import scalaz.Scalaz._

import com.precog.analytics._
//import com.precog.api.{ReportGridConfig, ReportGridClient, HttpClient, Server} 
//import com.precog.api.blueeyes.ReportGrid
import com.precog.ct._
import com.precog.ct.Mult._
import com.precog.ct.Mult.MDouble._
import service._

import BijectionsChunkJson._
import BijectionsChunkString._
import BijectionsChunkFutureJson._

//import rosetta.json.blueeyes._

import com.precog.ingest.api._

case class PastClock(duration: org.joda.time.Duration) extends Clock {
  def now() = new DateTime().minus(duration)
  def instant() = now().toInstant
  def nanoTime = sys.error("nanotime not available in the past")
}

trait TestTokens {
  val TestToken = Token(
    tokenId        = "C7A18C95-3619-415B-A89B-4CE47693E4CC",
    parentTokenId  = Some(Token.Root.tokenId),
    accountTokenId = "C7A18C95-3619-415B-A89B-4CE47693E4CC",
    path           = Path("unittest"),
    permissions    = Permissions(true, true, true, true),
    expires        = Token.Never,
    limits         = Limits(order = 2, depth = 5, limit = 20, tags = 2, rollup = 2)
  )

 val TrackingToken = Token(
    tokenId        = "DB6DEF4F-678A-4F7D-9897-F920762887F1",
    parentTokenId  = Some(Token.Root.tokenId),
    accountTokenId = "DB6DEF4F-678A-4F7D-9897-F920762887F1",
    path           = Path("__usage_tracking__"),
    permissions    = Permissions(true, true, true, true),
    expires        = Token.Never,
    limits         = Limits(order = 1, depth = 2, limit = 5, tags = 1, rollup = 2, lossless=false)
 )
}

trait TestIngestService extends BlueEyesServiceSpecification with IngestService with LocalMongo with TestTokens {

  val requestLoggingData = """
    requestLog {
      enabled = true
      fields = "time cs-method cs-uri sc-status cs-content"
    }
  """

  override val clock = Clock.System

  override val configuration = "services{ingest{v1{" + requestLoggingData + mongoConfigFileData + "}}}"

  override def mongoFactory(config: ConfigMap): Mongo = RealMongo(config)
  //override def mongoFactory(config: ConfigMap): Mongo = new MockMongo()

  //def auditClient(config: ConfigMap) = external.NoopTrackingClient

  def tokenManager(database: Database, tokensCollection: MongoCollection, deletedTokensCollection: MongoCollection): TokenManager = {
    val mgr = new TokenManager(database, tokensCollection, deletedTokensCollection) 
    mgr.tokenCache.put(TestToken.tokenId, TestToken)
    mgr.tokenCache.put(TrackingToken.tokenId, TrackingToken)
    mgr
  }

  def storageReporting(config: ConfigMap) = {
    //val testServer = Server("/")

    new ReportGridStorageReporting(TrackingToken.tokenId) 
  }

  val messaging = new CollectingMessaging

  def eventStoreFactory(configMap: ConfigMap): EventStore = {
    
    val defaultAddresses = NonEmptyList(MailboxAddress(0))

    val routeTable = new ConstantRouteTable(defaultAddresses)

    new EventStore(new EventRouter(routeTable, messaging), 0)
    
  }

  lazy val jsonTestService = service.contentType[JValue](application/(MimeTypes.json)).
                                     query("tokenId", TestToken.tokenId)

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(20, Duration(1, "second"))
  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))
}

class IngestServiceSpec extends TestIngestService with FutureMatchers {
  val genTimeClock = clock 

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
