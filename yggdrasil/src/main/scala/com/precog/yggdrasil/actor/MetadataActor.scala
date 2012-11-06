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
package com.precog.yggdrasil
package actor 

import com.precog.common.json._
import metadata._
import ColumnMetadata._

import com.precog.util._
import com.precog.common._

import com.weiglewilczek.slf4s.Logging
import org.slf4j.MDC

import blueeyes.json._
import blueeyes.json.JsonAST._

import blueeyes.json.serialization.Decomposer
import blueeyes.json.serialization.DefaultSerialization._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.dispatch.{ExecutionContext, Future, MessageDispatcher}

import scalaz.{Failure, Success, Validation, Show}
import scalaz.effect._
import scalaz.syntax.traverse._
import scalaz.syntax.semigroup._
import scalaz.syntax.show._
import scalaz.std.list._
import scalaz.std.map._
import scalaz.std.set._

class MetadataActor(shardId: String, storage: MetadataStorage, checkpointCoordination: CheckpointCoordination, initialCheckpoint: Option[YggCheckpoint]) extends Actor with Logging { metadataActor =>
  import ProjectionMetadata._
  
  private var messageClock: VectorClock = initialCheckpoint map { _.messageClock } getOrElse { VectorClock.empty }
  private var kafkaOffset: Option[Long] = initialCheckpoint map { _.offset }
  private var projections: Map[ProjectionDescriptor, ColumnMetadata] = Map()
  private var dirty: Set[ProjectionDescriptor] = Set()
  private var flushRequests = 0
  private var flushesComplete = 0

  override def preStart(): Unit = {
    logger.info("Loading yggCheckpoint...")

/*
    checkpointCoordination.loadYggCheckpoint(shardId) match {
      case Some(Success(checkpoint)) =>
        messageClock = checkpoint.messageClock
        kafkaOffset = Some(checkpoint.offset)
        logger.info("Successfully loaded checkpoint " + messageClock + " and kafka offset " + kafkaOffset)

      case Some(Failure(errors)) =>
        // TODO: This could be normal state on the first startup of a shard
        sys.error("Unable to load Kafka checkpoint: " + errors)

      case None =>
        logger.warn("No checkpoint loaded")
        messageClock = VectorClock.empty
        kafkaOffset = None
    } 
    */

    logger.info("MetadataActor yggCheckpoint load complete")
  }

  override def postStop(): Unit = {
    flush(None).unsafePerformIO
  }

  private val ingestBatchId = new java.util.concurrent.atomic.AtomicInteger()

  def receive = {
    case Status => logger.trace(Status.toString); sender ! status

    case msg @ IngestBatchMetadata(updates, batchClock, batchOffset) => {
      //logger.trace(msg.toString)
      MDC.put("metadata_batch", ingestBatchId.getAndIncrement().toString)
      for(update <- updates) update match {
        case (descriptor, Some(metadata)) =>
          val newMetadata = projections.get(descriptor).map { _ |+| metadata }.getOrElse(metadata)
          logger.trace("Updating descriptor from %s to %s".format(projections.get(descriptor), newMetadata))
          projections += (descriptor -> newMetadata)
          dirty += descriptor
        case (descriptor, None) =>
          logger.trace("Archive metadata on %s".format(descriptor))
          flush(None).flatMap(_ => storage.archiveMetadata(descriptor)).unsafePerformIO
          projections -= descriptor
          dirty -= descriptor
      }
      MDC.remove("metadata_batch")
      
      messageClock = messageClock |+| batchClock
      kafkaOffset = batchOffset orElse kafkaOffset
    }
   
    case msg @ FindChildren(path) => 
      logger.trace(msg.toString)
      sender ! storage.findChildren(path)
    
    case msg @ FindSelectors(path) => 
      logger.trace(msg.toString)
      sender ! storage.findSelectors(path)

    case msg @ FindDescriptors(path, selector) => 
      logger.trace(msg.toString)
      val result = findDescriptors(path, selector).unsafePerformIO
      logger.trace("Found descriptors: " + result)
      sender ! result

    case msg @ FindPathMetadata(path, selector) => 
      logger.trace(msg.toString)
      sender ! storage.findPathMetadata(path, selector, columnMetadataFor).unsafePerformIO

    case msg @ FindDescriptorRoot(descriptor, createOk) => 
      logger.trace(msg.toString)
      sender ! storage.findDescriptorRoot(descriptor, createOk)
    
    case msg @ FindDescriptorArchive(descriptor) => 
      logger.trace(msg.toString)
      sender ! storage.findArchiveRoot(descriptor)
    
    case msg @ FlushMetadata => 
      flush(Some(sender)).unsafePerformIO

    case msg @ GetCurrentCheckpoint => 
      logger.trace(msg.toString)
      sender ! kafkaOffset.map(YggCheckpoint(_, messageClock)) 
  }

  private def flush(replyTo: Option[ActorRef]): IO[PrecogUnit] = {
    flushRequests += 1
    logger.debug("Flushing metadata (%s, request %d)...".format(if (replyTo.nonEmpty) "scheduled" else "forced", flushRequests))

    val io: IO[List[PrecogUnit]] = fullDataFor(dirty) flatMap { 
      _.toList.map({ case (desc, meta) => storage.updateMetadata(desc, MetadataRecord(meta, messageClock)) }).sequence[IO, PrecogUnit]
    }

    // if some metadata fails to be written and we consequently don't write the checkpoint,
    // then the restore process for each projection will need to skip all message ids prior
    // to the checkpoint clock associated with that metadata
    io.catchLeft.map { 
      case Left(error) =>
        logger.error("Error saving metadata for flush request %d; checkpoint at offset %s, clock %s ignored.".format(flushRequests, kafkaOffset.toString, messageClock.toString), error)
          
      case Right(_) =>
        for (offset <- kafkaOffset) checkpointCoordination.saveYggCheckpoint(shardId, YggCheckpoint(offset, messageClock))
        logger.debug("Flush " + flushRequests + " complete for projections: \n" + dirty.map(_.shows).mkString("\t", "\t\n", "\n"))
        dirty = Set()
        replyTo foreach { _ ! () }
    }.map(_ => PrecogUnit)
  }

  def status: JValue = JObject(JField("Metadata", JObject(JField("state", JString("Ice cream!")) :: Nil)) :: Nil) // TODO: no, really...

  def findDescriptors(path: Path, selector: CPath): IO[Map[ProjectionDescriptor, ColumnMetadata]] = {
    @inline def matches(path: Path, selector: CPath) = {
      (col: ColumnDescriptor) => col.path == path && (col.selector.nodes startsWith selector.nodes)
    }

    fullDataFor(storage.findDescriptors(_.columns.exists(matches(path, selector))))
  } 

  def ensureMetadataCached(descriptor: ProjectionDescriptor): IO[PrecogUnit] = {
    if (projections.contains(descriptor)) {
      IO(PrecogUnit)
    } else storage.getMetadata(descriptor) map { 
      case MetadataRecord(metadata, clock) => 
        projections += (descriptor -> metadata)
        PrecogUnit
    }
  }

  def fullDataFor(projs: Set[ProjectionDescriptor]): IO[Map[ProjectionDescriptor, ColumnMetadata]] = {
    projs.map(descriptor => columnMetadataFor(descriptor) map (descriptor -> _)).sequence.map(_.toMap)
  }

  private def columnMetadataFor(descriptor: ProjectionDescriptor): IO[ColumnMetadata] = {
    projections.get(descriptor).map(IO(_)).getOrElse {
      storage.getMetadata(descriptor) map {
        case MetadataRecord(metadata, clock) => 
          projections += (descriptor -> metadata)
          metadata
      }
    }
  }
}


object ProjectionMetadata {
  import metadata._
  import ProjectionInsert.Row

  def columnMetadata(desc: ProjectionDescriptor, rows: Seq[Row]): ColumnMetadata = {
    rows.foldLeft(ColumnMetadata.Empty) { 
      case (acc, Row(_, values, metadata)) => acc |+| columnMetadata(desc, values, metadata) 
    }
  }

  def columnMetadata(desc: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]]): ColumnMetadata = {
    def addValueMetadata(values: Seq[CValue], metadata: Seq[MetadataMap]): Seq[MetadataMap] = {
      values zip metadata map { case (value, mmap) => valueStats(value).map(vs => mmap + (vs.metadataType -> vs)).getOrElse(mmap) }
    }

    val userAndValueMetadata: Seq[MetadataMap] = addValueMetadata(values, metadata.map { Metadata.toTypedMap _ })
    (desc.columns zip userAndValueMetadata).toMap
  }

  def updateColumnMetadata(initialMetadata: ColumnMetadata, desc: ProjectionDescriptor, values: Seq[CValue], metadata: Seq[Set[Metadata]]): ColumnMetadata = {
    columnMetadata(desc, values, metadata).foldLeft(initialMetadata) { 
      case (acc, (col, newColMetadata)) =>
        val updatedMetadata = acc.get(col) map { _ |+| newColMetadata } getOrElse { newColMetadata }

        acc + (col -> updatedMetadata)
    }
  }

  def initMetadata(desc: ProjectionDescriptor): ColumnMetadata = {
    desc.columns.foldLeft( Map[ColumnDescriptor, MetadataMap]() ) {
      (acc, col) => acc + (col -> Map[MetadataType, Metadata]())
    }
  }

  def valueStats(cval: CValue): Option[Metadata] = cval match { 
    case CString(s)  => Some(StringValueStats(1, s, s))
    case CBoolean(b) => Some(BooleanValueStats(1, if(b) 1 else 0))
    //case CInt(i)     => Some(LongValueStats(1, i, i))
    case CLong(l)    => Some(LongValueStats(1, l, l))
    //case CFloat(f)   => Some(DoubleValueStats(1, f, f))
    case CDouble(d)  => Some(DoubleValueStats(1, d, d))
    case CNum(bd)    => Some(BigDecimalValueStats(1, bd, bd))
    case _           => None
  }
}   

sealed trait ShardMetadataAction

case class ExpectedEventActions(eventId: EventId, count: Int) extends ShardMetadataAction

case class FindChildren(path: Path) extends ShardMetadataAction
case class FindSelectors(path: Path) extends ShardMetadataAction
case class FindDescriptors(path: Path, selector: CPath) extends ShardMetadataAction
case class FindPathMetadata(path: Path, selector: CPath) extends ShardMetadataAction
case class FindDescriptorRoot(desc: ProjectionDescriptor, createOk: Boolean) extends ShardMetadataAction
case class FindDescriptorArchive(desc: ProjectionDescriptor) extends ShardMetadataAction
case class MetadataSaved(saved: Set[ProjectionDescriptor]) extends ShardMetadataAction
case object GetCurrentCheckpoint

case class IngestBatchMetadata(updates: Seq[(ProjectionDescriptor, Option[ColumnMetadata])],  messageClock: VectorClock, kafkaOffset: Option[Long]) extends ShardMetadataAction
case object FlushMetadata extends ShardMetadataAction
