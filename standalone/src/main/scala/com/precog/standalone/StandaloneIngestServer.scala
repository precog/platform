package com.precog.standalone

import akka.dispatch.Future

import blueeyes.BlueEyesServer
import blueeyes.bkka.AkkaTypeClasses
import blueeyes.util.Clock

import scalaz.Monad

import com.precog.common.security.StaticAPIKeyManagerComponent
import com.precog.ingest.EventService
import com.precog.ingest.kafka.KafkaEventStoreComponent

object StandaloneIngestServer
    extends BlueEyesServer
    with EventService
    with StaticAPIKeyManagerComponent
    with KafkaEventStoreComponent {
  val clock = Clock.System

  implicit val asyncContext = defaultFutureDispatch
  implicit val M: Monad[Future] = AkkaTypeClasses.futureApplicative(asyncContext)
}
