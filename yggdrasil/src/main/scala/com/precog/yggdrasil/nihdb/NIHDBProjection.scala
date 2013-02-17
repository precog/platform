package com.precog.yggdrasil
package nihdb

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.json._
import com.precog.niflheim._
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
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.Extractor._

import com.weiglewilczek.slf4s.Logging

import org.objectweb.howl.log._

import scalaz._
import scalaz.effect.IO

import shapeless._

import java.io.{File, FileNotFoundException}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable.SortedMap
import scala.collection.JavaConverters._

sealed trait NIHActorMessage

// FIXME: dedup this

case class FindProjectionDescriptor(path: Path)

sealed trait ProjectionUpdate {
  def descriptor: ProjectionDescriptor
  def path: Path = descriptor.path
}

case class ProjectionInsert(descriptor: ProjectionDescriptor, id: EventId, values: JValue) extends ProjectionUpdate

case class ProjectionBatchInsert(descriptor: ProjectionDescriptor, id: EventId, values: Seq[JValue]) extends ProjectionUpdate

case class ProjectionArchive(descriptor: ProjectionDescriptor, id: EventId) extends ProjectionUpdate

case class ProjectionGetBlock(descriptor: ProjectionDescriptor, id: Option[Long], columns: Option[Set[CPath]])

case object ProjectionGetStats
case class ProjectionStats(cooked: Int, pending: Int, rawSize: Int)

case object ProjectionGetStructure
case class ProjectionStructure(columns: Set[ColumnRef])

class NIHDBProjectionsActor(
    rootDir: File,
    chef: ActorRef,
    cookThreshold: Int,
    actorSystem: ActorSystem,
    actorTimeout: Duration
    ) extends Actor {

  private def baseDir(path: Path): File =
    new File(rootDir, path.toString)

  private val projections = mutable.Map.empty[Path, Projection]

  private def getProjection(path: Path): Projection = {
    projections.getOrElseUpdate(path, {
      new NIHDBProjection(baseDir(path), path,
        chef, cookThreshold, actorSystem, actorTimeout)
    })
  }

  def receive = {
    case FindProjectionDescriptor(path) =>
      // Option[ProjectionDescriptor]
    case update: ProjectionUpdate =>
      getProjection(update.path) ! 
  }
}

/**
  *  Projection for NIH DB files
  *
  * @param cookThreshold The threshold, in rows, of raw data for cooking a raw store file
  */
class NIHDBProjection(val baseDir: File, val descriptor: ProjectionDescriptor, chef: ActorRef, cookThreshold: Int, actorSystem: ActorSystem, actorTimeout: Duration)
    extends RawProjectionLike[Future, Long, Slice]
    with AskSupport
    with GracefulStopSupport
    with Logging { projection =>

  private implicit val asyncContext: ExecutionContext = actorSystem.dispatcher
  private implicit val timeout = Timeout.durationToTimeout(actorTimeout)

  private[this] val actor = actorSystem.actorOf(Props(new NIHDBActor(baseDir, descriptor, chef, cookThreshold)))

  def getBlockAfter(id: Option[Long], columns: Set[ColumnRef])(implicit M: Monad[Future]): Future[Option[BlockProjectionData[Long, Slice]]] = {
    // FIXME: We probably want to change this semantic throughout Yggdrasil
    val constraint = if (columns.size > 0) Some(columns.map(_.selector)) else None
    (actor ? ProjectionGetBlock(descriptor, id, constraint)).mapTo[Option[BlockProjectionData[Long, Slice]]]
  }

  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): Future[PrecogUnit] = {
    Promise.successful(PrecogUnit)
  }

  def insert(id : Identities, v : Seq[JValue]): Future[PrecogUnit] = {
    actor ! ProjectionInsert(descriptor, id, v)
    Promise.successful(PrecogUnit)
  }

  def structure: Future[Set[ColumnRef]] = (actor ? ProjectionGetStructure).mapTo[Set[ColumnRef]]

  // NOOP. For now we sync *everything*
  def commit: Future[PrecogUnit] = Promise.successful(PrecogUnit)

  def stats: Future[ProjectionStats] = (actor ? ProjectionGetStats).mapTo[ProjectionStats]

  def close() = {
    gracefulStop(actor, actorTimeout)(actorSystem).map {_ => PrecogUnit }
  }
}

class NIHDBActor(val baseDir: File, val descriptor: ProjectionDescriptor, chef: ActorRef, cookThreshold: Int)
    extends Actor
    with Logging {
  private case class BlockState(cooked: List[CookedReader], pending: Map[Long, StorageReader], rawLog: RawHandler)

  private[this] val workLock = FileLock(baseDir, "NIHProjection")

  private[this] val cookedDir = new File(baseDir, "cooked")
  private[this] val rawDir    = new File(baseDir, "raw")
  private[this] val cookedMapFile = new File(baseDir, "cookedMap.json")

  private[this] val cookSequence = new AtomicLong

  def initDirs(f: File) {
    if (!f.isDirectory)
      f.mkdirs() || throw new IOException("Failed to create %s" format f)
  }

  try {
    initDirs(cookedDir)
    initDirs(rawDir)
  } catch { case t: Exception =>
    logger.error("Failed to set up base directories for %s".format(descriptor), t)
    throw t
  }

  logger.debug("Opening log in " + baseDir)
  private[this] val txLog = new CookStateLog(baseDir)

  private[this] var currentState =
    if (cookedMapFile.exists) {
      ProjectionState.fromFile(cookedMapFile) match {
        case Success(state) => state

        case Failure(error) =>
          logger.error("Failed to load state! " + error.message)
          throw new Exception(error.message)
      }
    } else {
      logger.info("No current descriptor found, creating fresh descriptor")
      ProjectionState.empty
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

  override def postStop() = {
    IO {
      txLog.close
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

      ProjectionState.toFile(currentState, cookedMapFile)
      txLog.completeCook(id)

    case ProjectionInsert(_, eventId, values) =>
      if (ids.length != 1) {
        logger.error("Cannot insert events with less/more than a single identity: " + ids.mkString("[", ",", "]"))
      } else {
        val pid = eventId.producerId
        val sid = eventId.sequenceId
        if (!currentState.producerThresholds.contains(pid) || sid > currentState.producerThresholds(pid)) {
          logger.debug("Inserting %d rows for %d:%d".format(values.length, pid, sid))
          blockState.rawLog.write(eventId.uid, values)

          // Update the producer thresholds for the rows. We know that ids only has one element due to the initial check
          currentState = currentState.copy(producerThresholds = updatedThresholds(currentState.producerThresholds, ids))

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
      }


    case ProjectionGetBlock(_, id, selectors) =>
      val blocks = id.map { i => currentBlocks.from(i).drop(1) }.getOrElse(currentBlocks)
      sender ! blocks.headOption.flatMap {
        case (id, reader) if reader.length > 0 => Some(BlockProjectionData(id, id, SegmentsWrapper(reader.snapshot(selectors))))
        case _ => None
      }

    case ProjectionGetStats =>
      sender ! ProjectionStats(blockState.cooked.length, blockState.pending.size, blockState.rawLog.length)

    case ProjectionGetStructure =>
      val perBlock: Set[ColumnRef] = currentBlocks.values.map { block: StorageReader => block.structure.map { case (s, t) => ColumnRef(s, t) } }.toSet.flatten
      sender ! ProjectionStructure(perBlock)
  }
}

case class ProjectionState(producerThresholds: Map[Int, Int], cookedMap: Map[Long, String]) {
  def readers(baseDir: File): List[CookedReader] =
    cookedMap.map { 
      case (id, metadataFile) => 
        CookedReader.load(new File(baseDir, metadataFile)) 
    }.toList
}

object ProjectionState {
  import Extractor.Error

  def empty = ProjectionState(Map.empty, Map.empty)

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
