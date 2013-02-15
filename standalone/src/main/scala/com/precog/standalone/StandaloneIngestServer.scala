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

object StandaloneIngestServer
    extends BlueEyesServer
    with EventService
    with AkkaDefaults {
  val executionContext = defaultFutureDispatch
  implicit val M: Monad[Future] = new FutureMonad(executionContext)

  val clock = Clock.System

  def configure(config: Configuration): (EventServiceDeps[Future], Stoppable)  = {
    val accountFinder0 = new StaticAccountFinder(config[String]("security.masterAccount.accountId"))
    val (eventStore0, stoppable) = KafkaEventStore(config, accountFinder0) getOrElse {
      sys.error("Invalid configuration: eventStore.central.zk.connect required")
    }

    val deps = EventServiceDeps[Future]( 
      apiKeyFinder = new StaticAPIKeyFinder[Future](config[String]("security.masterAccount.apiKey")),
      accountFinder = accountFinder0,
      eventStore = eventStore0,
      jobManager = new InMemoryJobManager[({ type λ[+α] = EitherT[Future, String, α] })#λ]()
    )

    (deps, stoppable)
  }
}
