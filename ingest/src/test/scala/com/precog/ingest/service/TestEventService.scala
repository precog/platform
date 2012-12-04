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

import com.precog.accounts._
import com.precog.daze._
import com.precog.common.{ Path, Event, EventMessage }
import com.precog.common.security._

import org.specs2.mutable.Specification
import org.specs2.specification._
import org.scalacheck.Gen._

import akka.actor.ActorSystem
import akka.dispatch.{ Future, ExecutionContext, Await }
import akka.util.Duration

import org.joda.time._

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

import scalaz._
import scalaz.Scalaz._

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

  val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)

  val apiKeyManager = new InMemoryAPIKeyManager[Future]
  def apiKeyManagerFactory(config: Configuration) = apiKeyManager
  
  val accountManager = new InMemoryAccountManager[Future]
  def accountManagerFactory(config: Configuration) = accountManager

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(0, Duration(1, "second"))

  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))

  val to = Duration(1, "seconds")
  val rootAPIKey = Await.result(apiKeyManager.rootAPIKey, to)
  
  val testAccount = Await.result(accountManager.newAccount("test@example.com", "open sesame", new DateTime, AccountPlan.Free) {
    case (accountId, path) => apiKeyManager.newStandardAPIKeyRecord(accountId, path).map(_.apiKey)
  }, to)
  
  val testAccountId = testAccount.accountId
  val testPath = testAccount.rootPath
  val testAPIKey = testAccount.apiKey
  
  val accessTest = Set[Permission](
    ReadPermission(testPath, Set("test")),
    ReducePermission(testPath, Set("test")),
    WritePermission(testPath, Set()),
    DeletePermission(testPath, Set())
  )
  
  val expiredAccount = Await.result(accountManager.newAccount("expired@example.com", "open sesame", new DateTime, AccountPlan.Free) {
    case (accountId, path) =>
      apiKeyManager.newStandardAPIKeyRecord(accountId, path).map(_.apiKey).flatMap { expiredAPIKey => 
        apiKeyManager.deriveAndAddGrant(None, None, testAPIKey, accessTest, expiredAPIKey, Some(new DateTime().minusYears(1000))).map(_ => expiredAPIKey)
      }
  }, to)
  
  val expiredAccountId = expiredAccount.accountId
  val expiredPath = expiredAccount.rootPath
  val expiredAPIKey = expiredAccount.apiKey
  
  val messaging = new CollectingMessaging

  def queryExecutorFactory(config: Configuration) = new NullQueryExecutor {
    lazy val actorSystem = ActorSystem("ingestServiceSpec")
    implicit lazy val executionContext = ExecutionContext.defaultExecutionContext(actorSystem)
  }

  def eventStoreFactory(config: Configuration): EventStore = {
    val defaultAddresses = NonEmptyList(MailboxAddress(0))

    val routeTable = new ConstantRouteTable(defaultAddresses)

    new KafkaEventStore(new EventRouter(routeTable, messaging), 0)
  }

  implicit def jValueToFutureJValue(j: JValue) = Future(j)

  def track[A](
    contentType: MimeType,
    apiKey: Option[APIKey],
    path: Path,
    ownerAccountId: Option[AccountId],
    sync: Boolean = true
  )(data: A)(implicit
    bi: A => Future[JValue],
    t: AsyncHttpTranscoder[A, ByteChunk]
  ): Future[(HttpResponse[JValue], List[Event])] = {
    val svc = client.contentType[A](contentType).
      path(if (sync) "/sync/fs/" else "/async/fs/")
    val queries = List(apiKey.map(("apiKey", _)), ownerAccountId.
      map(("ownerAccountId", _))).sequence
    val svcWithQueries = queries.map(svc.queries(_ :_*)).getOrElse(svc)

    messaging.messages.clear()
    
    for {
      response <- svcWithQueries.post[A](path.toString)(data)
      content <- response.content map (a => bi(a) map (Some(_))) getOrElse Future(None)
    } yield {
      (response.copy(content = content), messaging.messages.toList collect {
        case EventMessage(_, event) => event
      })
    }
  }
}
