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
package com.precog.niflheim

import com.precog.common._
import com.precog.common.accounts.AccountId
import com.precog.common.ingest.EventId
import com.precog.common.json._
import com.precog.common.security.Authorities
import com.precog.util._

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.dispatch.{ExecutionContext, Future, Promise}
import akka.pattern.{AskSupport, GracefulStopSupport}
import akka.util.{Duration, Timeout}

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.Extractor._

import com.weiglewilczek.slf4s.Logging

import org.objectweb.howl.log._

import scalaz._
import scalaz.effect.IO
import scalaz.syntax.monoid._

import java.io.{File, FileNotFoundException, IOException}
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable.SortedMap
import scala.collection.JavaConverters._

import shapeless._

case class Insert(id: Long, values: Seq[JValue], ownerAccountId: AccountId)

case object GetSnapshot

case class Block(id: Long, segments: Seq[Segment], stable: Boolean)

case object GetStatus
case class Status(cooked: Int, pending: Int, rawSize: Int)

case object GetStructure
case class Structure(columns: Set[(CPath, CType)])

case object GetAuthorities

object NIHDB

class NIHDB(val baseDir: File, chef: ActorRef,
    cookThreshold: Int, timeout: Timeout)(implicit
    actorSystem: ActorSystem) extends GracefulStopSupport with AskSupport {

  private implicit val asyncContext: ExecutionContext = actorSystem.dispatcher
  private implicit val impTimeout = timeout

  private val actor = actorSystem.actorOf(Props(
    new NIHDBActor(baseDir, chef, cookThreshold)))

  def authorities: Future[Authorities] =
    (actor ? GetAuthorities).mapTo[Authorities]

  def insert(id: Long, values: Seq[JValue], ownerAccountId: AccountId): Future[PrecogUnit] =
    (actor ? Insert(id, values, ownerAccountId)) map { _ => PrecogUnit }

  def getSnapshot(): Future[NIHDBSnapshot] =
    (actor ? GetSnapshot).mapTo[NIHDBSnapshot]

  // TODO: We should require an `Option[Set[(CPath, CType)]]`.
  def getBlockAfter(id: Option[Long], cols: Option[Set[CPath]]): Future[Option[Block]] =
    getSnapshot().map(_.getBlockAfter(id, cols))

  def getBlock(id: Option[Long], cols: Option[Set[CPath]]): Future[Option[Block]] =
    getSnapshot().map(_.getBlock(id, cols))

  def length: Future[Long] =
    getSnapshot().map(_.count())

  def status: Future[Status] =
    (actor ? GetStatus).mapTo[Status]

  def structure: Future[Set[(CPath, CType)]] =
    getSnapshot().map(_.structure)

  /**
   * Returns the total number of defined objects for a given `CPath` *mask*.
   * Since this punches holes in our rows, it is not simply the length of the
   * block. Instead we count the number of rows that have at least one defined
   * value at each path (and their children).
   */
  def count(paths0: Option[Set[CPath]]): Future[Long] =
    getSnapshot().map(_.count(paths0))

  // def reduce[A](from: Option[Long], cols: Option[Set[(CPath, CType)]])(reduction: Reduction[A]): Future[Option[A]]

  def close(): Future[PrecogUnit] =
    gracefulStop(actor, timeout.duration)(actorSystem).map { _ => PrecogUnit }
}

object NIHDBActor {
  final val descriptorFilename = "NIHDBDescriptor.json"
  final val cookedSubdir = "cooked_blocks"
  final val rawSubdir = "raw_blocks"

  private[niflheim] final val escapeSuffix = "_byUser"

  def escapePath(path: Path) =
    Path(path.elements.map {
      case needsEscape if needsEscape == cookedSubdir || needsEscape == rawSubdir || needsEscape.endsWith(escapeSuffix) =>
        needsEscape + escapeSuffix
      case fine => fine
    }.toList)

  def unescapePath(path: Path) =
    Path(path.elements.map {
      case escaped if escaped.endsWith(escapeSuffix) =>
        escaped.substring(0, escaped.length - escapeSuffix.length)
      case fine => fine
    }.toList)

  final def hasProjection(dir: File) = (new File(dir, descriptorFilename)).exists
}

class NIHDBActor(baseDir: File, chef: ActorRef, cookThreshold: Int)
    extends Actor
    with Logging {
  private case class BlockState(cooked: List[CookedReader], pending: Map[Long, StorageReader], rawLog: RawHandler)

  import NIHDBActor._

  assert(cookThreshold > 0)
  assert(cookThreshold < (1 << 16))

  private[this] val workLock = FileLock(baseDir, "NIHProjection")

  private[this] val cookedDir = new File(baseDir, cookedSubdir)
  private[this] val rawDir    = new File(baseDir, rawSubdir)
  private[this] val descriptorFile = new File(baseDir, descriptorFilename)

  private[this] val cookSequence = new AtomicLong

  def initDirs(f: File) {
    if (!f.isDirectory) {
      Files.createDirectories(f.toPath)
    }
  }

  try {
    initDirs(cookedDir)
    initDirs(rawDir)
  } catch { case t: Exception =>
    logger.error("Failed to set up base directory: " + baseDir, t)
    throw t
  }

  logger.debug("Opening log in " + baseDir)
  private[this] val txLog = new CookStateLog(baseDir)

  private[this] var currentState =
    if (descriptorFile.exists) {
      ProjectionState.fromFile(descriptorFile) match {
        case Success(state) => state

        case Failure(error) =>
          logger.error("Failed to load state! " + error.message)
          throw new Exception(error.message)
      }
    } else {
      logger.info("No current descriptor found, creating fresh descriptor")
      ProjectionState.empty.tap {
        s => ProjectionState.toFile(s, descriptorFile)
      }
    }

  private[this] var blockState: BlockState = {
    // We'll need to update our current thresholds based on what we read out of any raw logs we open
    var thresholds = currentState.producerThresholds

    val currentRawFile = rawFileFor(txLog.currentBlockId)
    val (currentLog, rawLogThresholds) = if (currentRawFile.exists) {
      val (handler, events, ok) = RawHandler.load(txLog.currentBlockId, currentRawFile)
      if (!ok) {
        logger.warn("Corruption detected and recovery performed on " + currentRawFile)
      }
      (handler, events)
    } else {
      (RawHandler.empty(txLog.currentBlockId, currentRawFile), Seq.empty[Long])
    }

    thresholds = updatedThresholds(thresholds, rawLogThresholds)

    val pendingCooks = txLog.pendingCookIds.map { id =>
      val (reader, eIds, ok) = RawHandler.load(id, rawFileFor(id))
      if (!ok) {
        logger.warn("Corruption detected and recovery performed on " + currentRawFile)
      }
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
    case (id, reader) => chef ! Prepare(id, cookSequence.getAndIncrement, cookedDir, reader)
  }

  def getSnapshot(): NIHDBSnapshot = NIHDBSnapshot(currentBlocks)

  override def postStop() = {
    IO {
      txLog.close
      ProjectionState.toFile(currentState, descriptorFile)
    }.except { case t: Throwable =>
      IO { logger.error("Error during close", t) }
    }.unsafePerformIO

    workLock.release
  }

  private def rawFileFor(seq: Long) = new File(rawDir, "%06x.raw".format(seq))

  private def computeBlockMap(current: BlockState) = {
    val allBlocks: List[StorageReader] = (current.cooked ++ current.pending.values :+ current.rawLog).sortBy(_.id)
    SortedMap(allBlocks.map { r => r.id -> r }.toSeq: _*)
  }

  def updatedThresholds(current: Map[Int, Int], ids: Seq[Long]): Map[Int, Int] = {
    (current.toSeq ++ ids.map {
      i => val EventId(p, s) = EventId.fromLong(i); (p -> s)
    }).groupBy(_._1).map { case (p, ids) => (p -> ids.map(_._2).max) }
  }

  override def receive = {
    case GetSnapshot =>
      sender ! getSnapshot()

    case Cooked(id, _, _, file) =>
      // This could be a replacement for an existing id, so we
      // ned to remove/close any existing cooked block with the same
      // ID
      blockState = blockState.copy(
        cooked = CookedReader.load(file) :: blockState.cooked.filterNot(_.id == id),
        pending = blockState.pending - id
      )

      currentBlocks = computeBlockMap(blockState)

      currentState = currentState.copy(
        cookedMap = currentState.cookedMap + (id -> file.getName)
      )

      ProjectionState.toFile(currentState, descriptorFile)
      txLog.completeCook(id)

    case Insert(eventId, values, ownerAccountId) =>
      // FIXME: Deal with AUTHORITIES
      val pid = EventId.producerId(eventId)
      val sid = EventId.sequenceId(eventId)
      if (!currentState.producerThresholds.contains(pid) || sid > currentState.producerThresholds(pid)) {
        logger.debug("Inserting %d rows for %d:%d at %s".format(values.length, pid, sid, baseDir.getCanonicalPath))
        blockState.rawLog.write(eventId, values)

        // Update the producer thresholds for the rows. We know that ids only has one element due to the initial check
        val oldOwners = currentState.authorities.ownerAccountIds
        currentState = currentState.copy(producerThresholds = updatedThresholds(currentState.producerThresholds, Seq(eventId)), authorities = currentState.authorities.expand(ownerAccountId))

        // Make sure that ownership is updated immediately on disk
        if (!oldOwners.contains(ownerAccountId)) {
          ProjectionState.toFile(currentState, descriptorFile)
        }

        if (blockState.rawLog.length >= cookThreshold) {
          blockState.rawLog.close
          val toCook = blockState.rawLog
          val newRaw = RawHandler.empty(toCook.id + 1, rawFileFor(toCook.id + 1))

          blockState = blockState.copy(pending = blockState.pending + (toCook.id -> toCook), rawLog = newRaw)
          txLog.startCook(toCook.id)
          chef ! Prepare(toCook.id, cookSequence.getAndIncrement, cookedDir, toCook)
        }
      } else {
        logger.debug("Skipping previously seen ID = %d:%d".format(pid, sid))
      }
      sender ! ()

    case GetStatus =>
      sender ! Status(blockState.cooked.length, blockState.pending.size, blockState.rawLog.length)

    case GetAuthorities =>
      sender ! currentState.authorities
  }
}

case class ProjectionState(producerThresholds: Map[Int, Int], cookedMap: Map[Long, String], authorities: Authorities) {
  def readers(baseDir: File): List[CookedReader] =
    cookedMap.map {
      case (id, metadataFile) =>
        CookedReader.load(new File(baseDir, metadataFile))
    }.toList
}

object ProjectionState {
  import Extractor.Error

  def empty = ProjectionState(Map.empty, Map.empty, Authorities.Empty)

  implicit val projectionStateIso = Iso.hlist(ProjectionState.apply _, ProjectionState.unapply _)

  // FIXME: Add version for this format
  val v1Schema = "producerThresholds" :: "cookedMap" :: "authorities" :: HNil

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
