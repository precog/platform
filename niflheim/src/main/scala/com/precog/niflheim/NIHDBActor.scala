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
import scalaz.Validation._
import scalaz.effect.IO
import scalaz.syntax.monoid._

import java.io.{File, FileNotFoundException, IOException}
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable.SortedMap
import scala.collection.JavaConverters._

import shapeless._

case class Insert(id: Long, values: Seq[JValue])

case object GetSnapshot

case class Block(id: Long, segments: Seq[Segment], stable: Boolean)

case object GetStatus
case class Status(cooked: Int, pending: Int, rawSize: Int)

case object GetStructure
case class Structure(columns: Set[(CPath, CType)])

case object GetAuthorities

object NIHDB {
  def create(chef: ActorRef, authorities: Authorities, baseDir: File, cookThreshold: Int, timeout: Timeout)(implicit actorSystem: ActorSystem) = {
    NIHDBActor.create(chef, authorities, baseDir, cookThreshold, timeout) map { _ map { actor => new NIHDB(actor, timeout) } }
  }

  def open(chef: ActorRef, baseDir: File, cookThreshold: Int, timeout: Timeout)(implicit actorSystem: ActorSystem) = {
    NIHDBActor.open(chef, baseDir, cookThreshold, timeout) map { _ map { _ map { case (authorities, actor) => (authorities, new NIHDB(actor, timeout)) } } }
  }
}

class NIHDB private (actor: ActorRef, timeout: Timeout)(implicit executor: ExecutionContext) extends GracefulStopSupport with AskSupport {
  private implicit val impTimeout = timeout

  def authorities: Future[Authorities] =
    (actor ? GetAuthorities).mapTo[Authorities]

  def insert(id: Long, values: Seq[JValue]): Future[PrecogUnit] =
    (actor ? Insert(id, values)) map { _ => PrecogUnit }

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

  def close(implicit actorSystem: ActorSystem): Future[PrecogUnit] =
    gracefulStop(actor, timeout.duration)(actorSystem).map { _ => PrecogUnit }
}

object NIHDBActor extends Logging {
  final val descriptorFilename = "NIHDBDescriptor.json"
  final val cookedSubdir = "cooked_blocks"
  final val rawSubdir = "raw_blocks"
  final val lockName = "NIHDBProjection"

  private[niflheim] final val escapeSuffix = "_byUser"

  private[niflheim] final val internalDirs =
    Set(cookedSubdir, rawSubdir, descriptorFilename, CookStateLog.logName + "_1.log", CookStateLog.logName + "_2.log",  lockName + ".lock", CookStateLog.lockName + ".lock")

  def create(chef: ActorRef, authorities: Authorities, baseDir: File, cookThreshold: Int, timeout: Timeout)(implicit actorSystem: ActorSystem): IO[Validation[Error, ActorRef]] = {
    val descriptorFile = new File(baseDir, descriptorFilename)
    val currentState: IO[Validation[Error, ProjectionState]] =
      if (descriptorFile.exists) {
        ProjectionState.fromFile(descriptorFile)
      } else {
        val state = ProjectionState.empty(authorities)
        for {
          _ <- IO { logger.info("No current descriptor found for " + baseDir + "; " + authorities + ", creating fresh descriptor") }
          _ <- ProjectionState.toFile(state, descriptorFile)
        } yield {
          success(state)
        }
      }

    currentState map { _ map { s => actorSystem.actorOf(Props(new NIHDBActor(s, baseDir, chef, cookThreshold))) } }
  }

  def readDescriptor(baseDir: File): IO[Option[Validation[Error, ProjectionState]]] = {
    val descriptorFile = new File(baseDir, descriptorFilename)
    if (descriptorFile.exists) {
      ProjectionState.fromFile(descriptorFile) map { Some(_) }
    } else {
      logger.warn("No projection found at " + baseDir)
      IO { None }
    }
  }

  def open(chef: ActorRef, baseDir: File, cookThreshold: Int, timeout: Timeout)(implicit actorSystem: ActorSystem): IO[Option[Validation[Error, (Authorities, ActorRef)]]] = {
    val currentState: IO[Option[Validation[Error, ProjectionState]]] = readDescriptor(baseDir)

    currentState map { _ map { _ map { s => (s.authorities, actorSystem.actorOf(Props(new NIHDBActor(s, baseDir, chef, cookThreshold)))) } } }
  }

  def escapePath(path: Path, toEscape: Set[String]) =
    Path(path.elements.map {
      case needsEscape if toEscape.contains(needsEscape) || needsEscape.endsWith(escapeSuffix) =>
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

class NIHDBActor private (private var currentState: ProjectionState, baseDir: File, chef: ActorRef, cookThreshold: Int)
    extends Actor
    with Logging {
  private case class BlockState(cooked: List[CookedReader], pending: Map[Long, StorageReader], rawLog: RawHandler)

  import NIHDBActor._

  assert(cookThreshold > 0)
  assert(cookThreshold < (1 << 16))

  private[this] val workLock = FileLock(baseDir, lockName)

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

  logger.debug("Current raw block id = " + txLog.currentBlockId)

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

  logger.debug("Initial block state = " + blockState)

  private[this] var currentBlocks: SortedMap[Long, StorageReader] = computeBlockMap(blockState)

  // Re-fire any restored pending cooks
  blockState.pending.foreach {
    case (id, reader) =>
      logger.debug("Restarting pending cook on block %s:%d".format(baseDir, id))
      chef ! Prepare(id, cookSequence.getAndIncrement, cookedDir, reader)
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

      logger.debug("Cook complete on %d".format(id))

      ProjectionState.toFile(currentState, descriptorFile).unsafePerformIO
      txLog.completeCook(id)

    case Insert(eventId, values) =>
      val pid = EventId.producerId(eventId)
      val sid = EventId.sequenceId(eventId)
      if (!currentState.producerThresholds.contains(pid) || sid > currentState.producerThresholds(pid)) {
        logger.debug("Inserting %d rows for %d:%d at %s".format(values.length, pid, sid, baseDir.getCanonicalPath))
        blockState.rawLog.write(eventId, values)

        // Update the producer thresholds for the rows. We know that ids only has one element due to the initial check
        currentState = currentState.copy(producerThresholds = updatedThresholds(currentState.producerThresholds, Seq(eventId)))

        if (blockState.rawLog.length >= cookThreshold) {
          logger.debug("Starting cook after threshold exceeded")
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

  def empty(authorities: Authorities) = ProjectionState(Map.empty, Map.empty, authorities)

  implicit val projectionStateIso = Iso.hlist(ProjectionState.apply _, ProjectionState.unapply _)

  // FIXME: Add version for this format
  val v1Schema = "producerThresholds" :: "cookedMap" :: "authorities" :: HNil

  implicit val stateDecomposer = decomposer[ProjectionState](v1Schema)
  implicit val stateExtractor  = extractor[ProjectionState](v1Schema)

  def fromFile(input: File): IO[Validation[Error, ProjectionState]] = IO {
    JParser.parseFromFile(input).bimap(Extractor.Thrown(_), x => x).flatMap { jv =>
      jv.validated[ProjectionState]
    }
  }

  def toFile(state: ProjectionState, output: File): IO[Boolean] = {
    IOUtils.safeWriteToFile(state.serialize.renderCompact, output)
  }
}
