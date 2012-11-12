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

import blueeyes.concurrent.test._

import blueeyes.core.data._
import blueeyes.bkka.{ AkkaDefaults, AkkaTypeClasses }
import blueeyes.core.service.test.BlueEyesServiceSpecification
import blueeyes.core.http.HttpResponse
import blueeyes.core.http.HttpStatus
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.http.MimeType
import blueeyes.core.http.MimeTypes
import blueeyes.core.http.MimeTypes._

import blueeyes.json._

trait TestIngestService extends
  BlueEyesServiceSpecification with
  IngestService with
  AkkaDefaults with
  AccountManagerClientComponent with
  MongoAPIKeyManagerComponent {
  val apiKeyManager = new InMemoryAPIKeyManager[Future]
  
  lazy val trackingAPIKey: APIKey = sys.error("FIXME")
  lazy val testAPIKey: APIKey = sys.error("FIXME")
  lazy val expiredAPIKey: APIKey = sys.error("FIXME")
  
  val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)

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

  def usageLoggingFactory(config: Configuration) = new ReportGridUsageLogging(trackingAPIKey) 

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

  override def apiKeyManagerFactory(config: Configuration) = apiKeyManager

  implicit def jValueToFutureJValue = new Bijection[JValue, Future[JValue]] {
    def apply(x: JValue) = Future(x)
    def unapply(f: Future[JValue]) = Await.result(f, Duration(500, "millis"))
  }

  def track[A](
      contentType: MimeType,
      sync: Boolean = true,
      apiKey: Option[String] = Some(testAPIKey),
      path: String = "unittest"
    )(data: A)(implicit
      bi: Bijection[A, Future[JValue]],
      bi2: Bijection[A, ByteChunk]): Future[(HttpResponse[JValue], List[Event])] = {
    val svc = service.contentType[A](contentType).path(if (sync) "/sync/fs/" else "/async/fs/")
    messaging.messages.clear()
    for {
      response <- apiKey.map(svc.query("apiKey", _)).getOrElse(svc).post[A](path)(data)
      content <- response.content map (a => bi(a) map (Some(_))) getOrElse Future(None)
    } yield {
      (response.copy(content = content), messaging.messages.toList collect {
        case EventMessage(_, event) => event
      })
    }

  }

  override implicit val defaultFutureTimeouts: FutureTimeouts = FutureTimeouts(20, Duration(1, "second"))
  val shortFutureTimeouts = FutureTimeouts(5, Duration(50, "millis"))
}

object TestIngestService {
  lazy val rootAPIKey: APIKey = sys.error("FIXME")
}
