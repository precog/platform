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
package nihdb

import com.precog.common._
import com.precog.common.json._
import com.precog.util._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.table._

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.dispatch.{ExecutionContext, Future, Promise}
import akka.pattern.{AskSupport, GracefulStopSupport}
import akka.util.{Duration, Timeout}

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._

import com.weiglewilczek.slf4s.Logging

import org.objectweb.howl.log._

import scalaz._
import scalaz.effect.IO

import shapeless._

import java.io.File

import scala.collection.immutable.SortedMap

sealed trait NIHActorMessage

case class CookRawRequest(blockId: Long, data: Slice) extends NIHActorMessage
case class CookedRawComplete(blockId: Long, files: List[File]) extends NIHActorMessage

// FIXME: dedup this
case class ProjectionGetBlock(descriptor: ProjectionDescriptor, id: Option[Long], columns: Set[ColumnDescriptor])

trait StorageReader {
  def slice: Slice

  def blockId: Long
  def rows: Long
}

object RawReader {
  def load(blockId: Long, file: File): (StorageReader, Seq[Long]) = null
}

trait CookedReader extends StorageReader {
  def files: List[File]
}

object CookedReader {
  def load(blockId: Long, files: List[File]): CookedReader = null
}

trait RawWriter extends StorageReader {
  def write(id: Long, rows: ProjectionInsert.Row): Unit = {}
  def close: Unit
}

object RawWriter {
  def empty(blockId: Long, file: File): RawWriter = null
  def load(blockId: Long, file: File): (RawWriter, Seq[Long]) = null
}

/**
  *  Projection for NIH DB files
  *
  * @param cookThreshold The threshold, in rows, of raw data for cooking a raw store file
  */
abstract class NIHDBProjection(val baseDir: File, val descriptor: ProjectionDescriptor, cooker: ActorRef, cookThreshold: Long, actorSystem: ActorSystem, actorTimeout: Duration)
    extends RawProjectionLike[Future, Long, Slice]
    with AskSupport
    with GracefulStopSupport
    with Logging { projection =>

  private implicit val asyncContext: ExecutionContext = actorSystem.dispatcher
  private implicit val timeout = Timeout.durationToTimeout(actorTimeout)

  private[this] val actor = actorSystem.actorOf(Props(new NIHDBActor(baseDir, descriptor, cooker, cookThreshold)))

  def getBlockAfter(id: Option[Long], columns: Set[ColumnDescriptor] = Set())(implicit M: Monad[Future]): Future[Option[BlockProjectionData[Long, Slice]]] = {
    (actor ? ProjectionGetBlock(descriptor, id, columns)).mapTo[Option[BlockProjectionData[Long, Slice]]]
  }

  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): Future[PrecogUnit] = {
    actor ! ProjectionInsert(descriptor, Seq(ProjectionInsert.Row(id, v, Seq())))
    Promise.successful(PrecogUnit)
  }

  // NOOP. For now we sync *everything*
  def commit: Future[PrecogUnit] = Promise.successful(PrecogUnit)

  def close = {
    gracefulStop(actor, actorTimeout)(actorSystem)
  }
}

class NIHDBActor(val baseDir: File, val descriptor: ProjectionDescriptor, cooker: ActorRef, cookThreshold: Long)
    extends Actor
    with Logging {
  private case class BlockState(cooked: List[CookedReader], pending: Map[Long, StorageReader], rawLog: RawWriter)

  private[this] val workLock = FileLock(baseDir, "NIHProjection")

  private[this] val cookedDir = new File(baseDir, "cooked")
  private[this] val rawDir    = new File(baseDir, "raw")
  private[this] val cookedMapFile = new File(baseDir, "cookedMap.json")

  (for {
    c <- IO { cookedDir.mkdirs() }
    r <- IO { rawDir.mkdirs() }
  } yield (c,r)).except {
    case t: Throwable =>
      logger.error("Failed to set up base directories for %s".format(descriptor), t)
      throw t
  }.unsafePerformIO

  private[this] val txLog = new CookStateLog(baseDir)

  private[this] var currentState = ProjectionState.fromFile(cookedMapFile) match {
    case Success(state) => state
    case Failure(error) =>
      logger.error("Failed to load state! " + error.message)
      error.die
  }

  private[this] var blockState: BlockState = {
    // We'll need to update our current thresholds based on what we read out of any raw logs we open
    var thresholds = currentState.producerThresholds

    val (currentLog, rawLogThresholds) = RawWriter.load(txLog.currentBlockId, rawFileFor(txLog.currentBlockId))

    thresholds = updatedThresholds(thresholds, rawLogThresholds)

    val pendingCooks = txLog.pendingCookIds.map { id =>
      val (reader, eIds) = RawReader.load(id, rawFileFor(id))
      thresholds = updatedThresholds(thresholds, eIds)
      (id, reader)
    }.toMap

    currentState = currentState.copy(producerThresholds = thresholds)

    // Restore the cooked map
    val cooked = currentState.readers(cookedDir)

    BlockState(cooked, pendingCooks, currentLog)
  }

  private[this] var currentBlocks: SortedMap[Long, StorageReader] = computeBlockMap(blockState)

  // Re-fire any restored pending cooks
  blockState.pending.foreach {
    case (blockId, reader) => cooker ! CookRawRequest(blockId, reader.slice)
  }

  override def postStop() = {
    IO { txLog.close }.except {
      case t: Throwable => IO { logger.error("Error during close", t) }
    }.unsafePerformIO

    workLock.release
  }

  private def rawFileFor(seq: Long) = new File(rawDir, "%06x.raw".format(seq))

  private def computeBlockMap(current: BlockState) = {
    val allBlocks: List[StorageReader] = (current.cooked ++ current.pending.values :+ current.rawLog).sortBy(_.blockId)
    SortedMap(allBlocks.map { r => r.blockId -> r }.toSeq: _*)
  }

  def updatedThresholds(current: Map[Int, Int], ids: Seq[Long]): Map[Int, Int] = {
    (current.toSeq ++ ids.map {
      i => val EventId(p, s) = EventId(i); (p -> s)
    }).groupBy(_._1).map { case (p, ids) => (p -> ids.map(_._2).max) }
  }

  override def receive = {
    case CookedRawComplete(blockId, files) =>
      // This could be a replacement for an existing blockId, so we
      // ned to remove/close any existing cooked block with the same
      // ID
      blockState = blockState.copy(
        cooked = CookedReader.load(blockId, files) :: blockState.cooked.filterNot(_.blockId == blockId),
        pending = blockState.pending - blockId
      )
      currentBlocks = computeBlockMap(blockState)
      ProjectionState.toFile(currentState, cookedMapFile)
      txLog.completeCook(blockId)

    case ProjectionInsert(_, rows) =>
      val rowsToInsert = rows.filter { row =>
        if (row.ids.length != 1) {
          logger.error("Cannot insert events with less/more than a single identity: " + row.ids.mkString("[", ",", "]"))
          false
        } else {
          val pid = EventId.producerId(row.ids(0))
          val sid = EventId.sequenceId(row.ids(0))
          !currentState.producerThresholds.contains(pid) ||
          sid > currentState.producerThresholds(pid)
        }
      }

      logger.debug("Inserting %d rows (discarded %d)".format(rowsToInsert.size, rows.size - rowsToInsert.size))

      rowsToInsert.foreach { row =>
        blockState.rawLog.write(row.ids(0), row)
      }

      // Update the producer thresholds for the rows
      currentState = currentState.copy(producerThresholds = updatedThresholds(currentState.producerThresholds, rowsToInsert.map(_.ids(0))))

      if (blockState.rawLog.rows >= cookThreshold) {
        blockState.rawLog.close
        val toCook = blockState.rawLog
        val newRaw = RawWriter.empty(toCook.blockId + 1, rawFileFor(toCook.blockId + 1))

        blockState = blockState.copy(pending = blockState.pending + (toCook.blockId -> toCook), rawLog = newRaw)
        txLog.startCook(toCook.blockId)
        cooker ! CookRawRequest(toCook.blockId, toCook.slice)
      }

    case ProjectionGetBlock(_, id, _) =>
      val blocks = id.map { i => currentBlocks.from(i).drop(1) }.getOrElse(currentBlocks)
      sender ! blocks.headOption.map {
        case (id, reader) => BlockProjectionData(id, id, reader.slice)
      }
  }
}

case class ProjectionState(producerThresholds: Map[Int, Int], cookedMap: Map[Long, List[String]]) {
  def readers(baseDir: File): List[CookedReader] =
    cookedMap.map { case (blockId, files) => CookedReader.load(blockId, files.map { new File(baseDir, _) }) }.toList
}

object ProjectionState {
  import Extractor.Error

  implicit val projectionStateIso = Iso.hlist(ProjectionState.apply _, ProjectionState.unapply _)

  // FIXME: Add version for this format
  val v1Schema = "producerThresholds" :: "cookedMap" :: HNil

  implicit val stateDecomposer = decomposer[ProjectionState](v1Schema)
  implicit val stateExtractor  = extractor[ProjectionState](v1Schema)

  def fromFile(input: File): Validation[Error, ProjectionState] = {
    JParser.parseFromFile(input).bimap(Extractor.Thrown(_), x => x).flatMap { jv =>
      jv.validated[ProjectionState]
    }
  }

  def toFile(state: ProjectionState, output: File) = {
    IOUtils.safeWriteToFile(state.serialize.renderCompact, output).unsafePerformIO
  }
}
