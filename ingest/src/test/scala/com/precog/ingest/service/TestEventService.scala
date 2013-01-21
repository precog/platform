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

import com.precog.common._
import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.util._

import org.specs2.mutable.Specification
import org.specs2.specification._
import org.scalacheck.Gen._

import akka.actor.ActorSystem
import akka.dispatch.{ Future, ExecutionContext, Await }
import akka.util._

import org.joda.time.DateTime

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import scalaz._
import scalaz.std.list._
import scalaz.std.option._
import scalaz.syntax.monad._
import scalaz.syntax.copointed._
import scalaz.syntax.traverse._

import blueeyes.akka_testing._
import blueeyes.bkka._
import blueeyes.core.data._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http._
import blueeyes.core.http.MimeTypes._
import blueeyes.core.service._
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.json._

trait TestEventService extends
  BlueEyesServiceSpecification with
  EventService with
  AkkaDefaults {
  
  val config = """
    security {
      test = true
      mongo {
        mock = true
        servers = [localhost]
        database = test
      }
    }
  """

  override val configuration = "services { ingest { v1 { " + config + " } } }"

  private val to = Duration(1, "seconds")

  val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] with Copointed[Future] = new FutureMonad(asyncContext) with Copointed[Future] {
    def copoint[A](m: Future[A]) = Await.result(m, to)
  }

  private val apiKeyManager = new InMemoryAPIKeyManager[Future]

  protected val rootAPIKey = Await.result(apiKeyManager.rootAPIKey, to)
  protected val testAccount = TestAccounts.newAccount("test@example.com", "open sesame", new DateTime, AccountPlan.Free, None) {
    case (accountId, path) => apiKeyManager.newStandardAPIKeyRecord(accountId, path).map(_.apiKey)
  } copoint

  private val accountFinder = new TestAccountFinder[Future](Map(testAccount.apiKey -> testAccount.accountId), Map(testAccount.accountId -> testAccount))

  def APIKeyFinder(config: Configuration) = new DirectAPIKeyFinder(apiKeyManager)
  
  def AccountFinder(config: Configuration) = accountFinder

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(0, Duration(1, "second"))

  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))

  
  val accessTest = Set[Permission](
    ReadPermission(testAccount.rootPath, Set("test")),
    ReducePermission(testAccount.rootPath, Set("test")),
    WritePermission(testAccount.rootPath, Set()),
    DeletePermission(testAccount.rootPath, Set())
  )
  
  val expiredAccount = TestAccounts.newAccount("expired@example.com", "open sesame", new DateTime, AccountPlan.Free, None) {
    case (accountId, path) =>
      apiKeyManager.newStandardAPIKeyRecord(accountId, path).map(_.apiKey).flatMap { expiredAPIKey => 
        apiKeyManager.deriveAndAddGrant(None, None, testAccount.apiKey, accessTest, expiredAPIKey, Some(new DateTime().minusYears(1000))).map(_ => expiredAPIKey)
      }
  } copoint
  
/*
  def queryExecutorFactory(config: Configuration) = new NullQueryExecutor {
    lazy val actorSystem = ActorSystem("ingestServiceSpec")
    implicit lazy val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  }
  */

  private val stored = scala.collection.mutable.ArrayBuffer.empty[Event]
  def EventStore(config: Configuration): EventStore = new EventStore {
    def save(action: Event, timeout: Timeout) = M.point { stored += action; PrecogUnit }
    def start() = M.point(PrecogUnit)
    def stop() = M.point(PrecogUnit)
  }

  def JobManager(config: Configuration): JobManager[Future] = new InMemoryJobManager[Future]

  implicit def jValueToFutureJValue(j: JValue) = Future(j)

  def track[A](contentType: MimeType, apiKey: Option[APIKey], path: Path, ownerAccountId: Option[AccountId], sync: Boolean = true, batch: Boolean = false)(data: A)(implicit
    bi: A => Future[JValue],
    t: AsyncHttpTranscoder[A, ByteChunk]
  ): Future[(HttpResponse[JValue], List[Ingest])] = {
    val svc = client.contentType[A](contentType).query("receipt", sync.toString).query("mode", if (batch) "batch" else "stream").path("/fs/")

    val queries = List(apiKey.map(("apiKey", _)), ownerAccountId.map(("ownerAccountId", _))).sequence

    val svcWithQueries = queries.map(svc.queries(_ :_*)).getOrElse(svc)

    stored.clear()
    for {
      response <- svcWithQueries.post[A](path.toString)(data)
      content <- response.content map (a => bi(a) map (Some(_))) getOrElse Future(None)
    } yield {
      (
        response.copy(content = content), 
        stored.toList collect { case in: Ingest => in }
      )
    }
  }
}
