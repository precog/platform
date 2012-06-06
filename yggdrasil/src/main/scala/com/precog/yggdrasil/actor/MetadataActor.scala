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

import metadata._
import ColumnMetadata._

import com.precog.util._
import com.precog.common._

import com.weiglewilczek.slf4s.Logging

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.JPath

import blueeyes.json.xschema.Decomposer
import blueeyes.json.xschema.DefaultSerialization._

import akka.actor.Actor
import akka.actor.ActorRef
import akka.dispatch.{ExecutionContext, Future, MessageDispatcher}

import scalaz.{Failure, Success, Validation}
import scalaz.syntax.semigroup._
import scalaz.std.map._

object MetadataActor {
  case class ResolvedSelector(selector: JPath, authorities: Authorities, descriptor: ProjectionDescriptor, metadata: ColumnMetadata) {
    def columnType: CType = descriptor.columns.find(_.selector == selector).map(_.valueType).get
  }
}

class MetadataActor(shardId: String, metadataStorage: MetadataStorage, checkpointCoordination: CheckpointCoordination) extends Actor with Logging { metadataActor =>
  import ProjectionMetadata._
  
  private var messageClock: VectorClock = _
  private var kafkaOffset: Option[Long] = None
  private var projections: Map[ProjectionDescriptor, ColumnMetadata] = Map()
  private var dirty: Set[ProjectionDescriptor] = Set()
  private var flushRequests = 0
  private var flushesComplete = 0

  override def preStart(): Unit = {
    logger.info("Loading yggCheckpoint...")

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

    logger.info("MetadataActor yggCheckpoint load complete")
  }

  def receive = {
    case Status => sender ! state.status

    case IngestBatchMetadata(patch, batchClock, batchOffset) => 
      projections = projections |+| patch
      dirty = dirty ++ patch.keySet
      messageClock = messageClock |+| batchClock
      kafkaOffset = batchOffset orElse kafkaOffset
   
    case msg @ FindChildren(path)                       => 
      logger.info(msg.toString)
      sender ! findChildren(path)
    
    case msg @ FindSelectors(path)                      => 
      logger.info(msg.toString)
      sender ! findSelectors(path)

    case msg @ FindDescriptors(path, selector)          => 
      logger.info(msg.toString)
      sender ! findDescriptors(path, selector)

    case msg @ FindPathMetadata(path, selector)         => 
      logger.info(msg.toString)
      sender ! findPathMetadata(path, selector)

    case msg @ FindDescriptorRoot(descriptor, createOk) => 
      logger.info(msg.toString)
      sender ! metadataStorage.findDescriptorRoot(descriptor, createOk)
    
    case msg @ FlushMetadata        => 
      flushRequests += 1
      logger.info("Flushing metadata (request %d)...".format(flushRequests))

      val io: List[IO[Unit]] = fullDataFor(dirty).map({ case (desc, meta) => storage.updateMetadata(desc, MetadataRecord(meta, messageClock)) })(collection.breakOut)

      // if some metadata fails to be written and we consequently don't write the checkpoint,
      // then the restore process for each projection will need to skip all message ids prior
      // to the checkpoint clock associated with that metadata
      val replyTo = sender
      io.sequence[IO, Unit].catchLeft map { 
        case Left(error) =>
          logger.error("Error saving metadata for flush request %d; checkpoint at offset %s, clock %s ignored.".format(flushRequests, state.kafkaOffset.toString, state.messageClock.toString), error)
          // todo: Should we reply to the sender here? Failing to do so will block shutdown.
            
        case Right(_) =>
          for (offset <- state.kafkaOffset) checkpointCoordination.saveYggCheckpoint(shardId, YggCheckpoint(offset, state.messageClock))
          logger.info("Flush " + flushRequests + " complete for projections: \n" + dirty.map(_.show).mkString("\t", "\t\n", "\n")
          dirty = Set()
          replyTo ! () 
      } unsafePerformIO

    case msg @ GetCurrentCheckpoint                     => 
      logger.info(msg.toString)
      sender ! YggCheckpoint(state.kafkaOffset.getOrElse(0l), state.messageClock) // TODO: Make this safe
  }

  def status: JValue = JObject(JField("Metadata", JObject(JField("state", JString("Ice cream!")) :: Nil)) :: Nil) // TODO: no, really...

  def findDescriptors(path: Path, selector: JPath): Map[ProjectionDescriptor, ColumnMetadata] = {
    @inline def isEqualOrChild(ref: JPath, test: JPath) = test.nodes startsWith ref.nodes

    @inline def matches(path: Path, selector: JPath) = (col: ColumnDescriptor) => {
      col.path == path && isEqualOrChild(selector, col.selector)
    }

    fullDataFor(metadataStorage.findDescriptors {
      descriptor => descriptor.columns.exists(matches(path, selector))
    })
  } 

  def ensureMetadataCached(descriptor: ProjectionDescriptor): IO[Unit] = {
    if (projections.contains(descriptor)) IO()
    else metadataStorage.getMetadata map { 
      case MetadataRecord(metadata, clock) => projections += (descriptor -> metadata)
    }
  }

  def fullDataFor(projs: Set[ProjectionDescriptor]): Map[ProjectionDescriptor, ColumnMetadata] = {
    projs.toList.map { descriptor => (descriptor -> columnMetadataFor(descriptor)) } toMap
  }

  private def columnMetadataFor(descriptor: ProjectionDescriptor): IO[ColumnMetadata] = 
    projections.get(descriptor).map(IO(_)).getOrElse {
      metadataStorage.getMetadata(descriptor) map {
        case Success(MetadataRecort(metadata, clock)) => 
          projections += (descriptor -> metadata)
          record.metadata
        
        case Failure(errors) => sys.error("Failed to load metadata for " + descriptor)
      }
    }

  def findPathMetadata(path: Path, selector: JPath): PathRoot = {
    logger.debug("Locating path metadata for " + path + " and " + selector)

    @inline def isLeaf(ref: JPath, test: JPath) = {
      (test.nodes startsWith ref.nodes) && 
      test.nodes.length - 1 == ref.nodes.length
    }
    
    @inline def isObjectBranch(ref: JPath, test: JPath) = {
      (test.nodes startsWith ref.nodes) && 
      test.nodes.length > ref.nodes.length &&
      (test.nodes(ref.nodes.length) match {
        case JPathField(_) => true
        case _             => false
      })
    }
    
    @inline def isArrayBranch(ref: JPath, test: JPath) = {
      (test.nodes startsWith ref.nodes) && 
      test.nodes.length > ref.nodes.length &&
      (test.nodes(ref.nodes.length) match {
        case JPathIndex(_) => true
        case _             => false
      })
    }

    def extractIndex(base: JPath, child: JPath): Int = child.nodes(base.length) match {
      case JPathIndex(i) => i
      case _             => sys.error("assertion failed") 
    }

    def extractName(base: JPath, child: JPath): String = child.nodes(base.length) match {
      case JPathField(n) => n 
      case _             => sys.error("unpossible")
    }

    def newIsLeaf(ref: JPath, test: JPath): Boolean = ref == test 

    def selectorPartition(sel: JPath, rss: Seq[ResolvedSelector]):
        (Seq[ResolvedSelector], Seq[ResolvedSelector], Set[Int], Set[String]) = {
      val (values, nonValues) = rss.partition(rs => newIsLeaf(sel, rs.selector))
      val (indexes, fields) = rss.foldLeft( (Set.empty[Int], Set.empty[String]) ) {
        case (acc @ (is, fs), rs) => if(isArrayBranch(sel, rs.selector)) {
          (is + extractIndex(sel, rs.selector), fs)
        } else if(isObjectBranch(sel, rs.selector)) {
          (is, fs + extractName(sel, rs.selector))
        } else {
          acc
        }
      }

      (values, nonValues, indexes, fields)
    }

    def convertValues(values: Seq[ResolvedSelector]): Set[PathMetadata] = {
      values.foldLeft(Map[(JPath, CType), (Authorities, Map[ProjectionDescriptor, ColumnMetadata])]()) {
        case (acc, rs @ ResolvedSelector(sel, auth, desc, meta)) => 
          val key = (sel, rs.columnType)
          val update = acc.get(key).map(_._2).getOrElse( Map.empty[ProjectionDescriptor, ColumnMetadata] ) + (desc -> meta)
          acc + (key -> (auth, update))
      }.map {
        case ((sel, colType), (auth, meta)) => PathValue(colType, auth, meta) 
      }(collection.breakOut)
    }

    def buildTree(branch: JPath, rs: Seq[ResolvedSelector]): Set[PathMetadata] = {
      val (values, nonValues, indexes, fields) = selectorPartition(branch, rs)

      val oval = convertValues(values)
      val oidx = indexes.map { idx => PathIndex(idx, buildTree(branch \ idx, nonValues)) }
      val ofld = fields.map { fld => PathField(fld, buildTree(branch \ fld, nonValues)) }

      oval ++ oidx ++ ofld
    }

    @inline def matching(path: Path, selector: JPath): Seq[ResolvedSelector] = {
      metadataStorage.flatMapDescriptors {
        descriptor => descriptor.columns.collect {
          case col @ ColumnDescriptor(_,sel, _,auth) if col.path == path && selector.nodes startsWith col.selector.nodes =>
            ResolvedSelector(sel, auth, descriptor, columnMetadataFor(descriptor))
        }
      }
    }

    PathRoot(buildTree(selector, matching(path, selector)))
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
    case CInt(i)     => Some(LongValueStats(1, i, i))
    case CLong(l)    => Some(LongValueStats(1, l, l))
    case CFloat(f)   => Some(DoubleValueStats(1, f, f))
    case CDouble(d)  => Some(DoubleValueStats(1, d, d))
    case CNum(bd)    => Some(BigDecimalValueStats(1, bd, bd))
    case _           => None
  }
}   

sealed trait ShardMetadataAction

case class ExpectedEventActions(eventId: EventId, count: Int) extends ShardMetadataAction

case class FindChildren(path: Path) extends ShardMetadataAction
case class FindSelectors(path: Path) extends ShardMetadataAction
case class FindDescriptors(path: Path, selector: JPath) extends ShardMetadataAction
case class FindPathMetadata(path: Path, selector: JPath) extends ShardMetadataAction
case class FindDescriptorRoot(desc: ProjectionDescriptor, createOk: Boolean) extends ShardMetadataAction
case class MetadataSaved(saved: Set[ProjectionDescriptor]) extends ShardMetadataAction
case object GetCurrentCheckpoint

case class IngestBatchMetadata(metadata: Map[ProjectionDescriptor, ColumnMetadata], messageClock: VectorClock, kafkaOffset: Option[Long]) extends ShardMetadataAction
case object FlushMetadata extends ShardMetadataAction

// Sort of a replacement for LocalMetadata that really uses the underlying machinery
class TestMetadataActorish(initial: Map[ProjectionDescriptor,ColumnMetadata], storage: MetadataStorage)(implicit val dispatcher: MessageDispatcher, context: ExecutionContext) extends StorageMetadata {
  private val state = new MetadataActor.State(storage, VectorClock.empty, None, initial)

  def findChildren(path: Path): Future[Set[Path]] = Future(storage.findChildren(path))
  def findSelectors(path: Path): Future[Seq[JPath]] = Future(storage.findSelectors(path))
  def findProjections(path: Path, selector: JPath): Future[Map[ProjectionDescriptor, ColumnMetadata]] = Future(state.findDescriptors(path, selector))
  def findPathMetadata(path: Path, selector: JPath): Future[PathRoot] = Future(state.findPathMetadata(path, selector))

}
