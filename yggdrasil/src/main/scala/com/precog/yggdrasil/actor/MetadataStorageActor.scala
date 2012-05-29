package com.precog.yggdrasil
package actor

import metadata._
import leveldb._

import com.precog.util._
import com.precog.common._

import blueeyes.json.Printer
import blueeyes.json.JsonParser
import blueeyes.json.JsonAST._
import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import akka.actor.Actor
import akka.dispatch.Future

import scalaz._
import scalaz.effect._
import scalaz.std.list._
import scalaz.syntax.apply._
import scalaz.syntax.traverse._

import com.weiglewilczek.slf4s.Logging

import java.io.File

case class SaveMetadata(metadata: Map[ProjectionDescriptor, ColumnMetadata], messageClock: VectorClock, kafkaOffset: Option[Long])

case class MetadataSaveComplete(messageClock: VectorClock, kafkaOffset: Option[Long])
case class MetadataSaveFailed(errors: List[Throwable])

class MetadataStorageActor(shardId: String, storage: MetadataStorage, checkpointCoordination: SystemCoordination) extends Actor with Logging {
  def receive = {
    // TODO: Does it make any sense to save metadata *without* a checkpoint?
    case SaveMetadata(metadata, messageClock, kafkaOffset) => 
      val replyTo = sender
      val io: List[IO[Validation[Throwable, Unit]]] = 
        metadata.map({ case (desc, meta) => storage.updateMetadata(desc, MetadataRecord(meta, messageClock)) })(collection.breakOut)

      // if some metadata fails to be written and we consequently don't write the checkpoint,
      // then the restore process for each projection will need to skip all message ids prior
      // to the checkpoint clock associated with that metadata
      io.sequence[IO, Validation[Throwable, Unit]] map { results => 
        logger.debug("SaveMetadata results = " + results)
        val errors = results.collect { case Failure(t) => t } 
        if (errors.isEmpty) {
          for (offset <- kafkaOffset) checkpointCoordination.saveYggCheckpoint(shardId, YggCheckpoint(offset, messageClock))
          //sender ! MetadataSaveComplete(messageClock, kafkaOffset)
          replyTo ! MetadataSaved(metadata.keySet)
        } else {
          replyTo ! MetadataSaveFailed(errors)
        }
      } unsafePerformIO
  }
}

