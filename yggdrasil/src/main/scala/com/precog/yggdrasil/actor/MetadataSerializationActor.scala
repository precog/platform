package com.precog.yggdrasil
package actor

import metadata._

import com.precog.common.util._

import akka.actor.Actor

import scalaz.effect._
import scalaz.Scalaz._

import com.weiglewilczek.slf4s.Logging

class MetadataSerializationActor(checkpoints: YggCheckpoints, metadataIO: MetadataIO) extends Actor with Logging {
  def receive = {
    case SaveMetadata(metadata, messageClock) => 
      logger.debug("Syncing metadata")
      metadata.toList.map {
        case (pd, md) => metadataIO(pd, md)
      }.sequence[IO, Unit].map(_ => ()).unsafePerformIO
      logger.debug("Registering metadata checkpoint: " + messageClock)
      checkpoints.metadataPersisted(messageClock)
  }
}

sealed trait MetadataSerializationAction

case class SaveMetadata(metadata: Map[ProjectionDescriptor, ColumnMetadata], messageClock: VectorClock) extends MetadataSerializationAction
