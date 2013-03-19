package com.precog.standalone

import akka.dispatch.Future

import blueeyes.BlueEyesServer
import blueeyes.bkka._
import blueeyes.util.Clock

import scalaz._

import com.precog.accounts._
import com.precog.common.client._
import com.precog.common.security._
import com.precog.common.accounts._
import com.precog.common.jobs._
import com.precog.ingest._
import com.precog.ingest.kafka._

import org.streum.configrity.Configuration

object StandaloneIngestServer extends StandaloneIngestServer with AkkaDefaults {
  val executionContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(executionContext)
  val clock = Clock.System
}

trait StandaloneIngestServer
    extends BlueEyesServer
    with EventService {

  def clock: Clock

  def configureEventService(config: Configuration): (EventServiceDeps[Future], Stoppable)  = {
    val apiKey = config[String]("security.masterAccount.apiKey")
    val apiKeyFinder0 = new StaticAPIKeyFinder[Future](apiKey)
    val accountFinder0 = new StaticAccountFinder[Future](config[String]("security.masterAccount.accountId"), apiKey, Some("/"))
    val permissionsFinder = new PermissionsFinder(apiKeyFinder0, accountFinder0, clock.instant())
    val (eventStore0, stoppable) = KafkaEventStore(config, permissionsFinder) getOrElse {
      sys.error("Invalid configuration: eventStore.central.zk.connect required")
    }

    val deps = EventServiceDeps[Future](
      apiKeyFinder = apiKeyFinder0,
      accountFinder = accountFinder0,
      eventStore = eventStore0,
      jobManager = new InMemoryJobManager[({ type λ[+α] = EitherT[Future, String, α] })#λ]()
    )

    (deps, stoppable)
  }
}
