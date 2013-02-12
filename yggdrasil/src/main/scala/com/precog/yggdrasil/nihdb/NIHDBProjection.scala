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

import java.io.File

import scala.collection.immutable.SortedMap

sealed trait NIHActorMessage

case class CookRawRequest(blockId: Long, data: Slice) extends NIHActorMessage
case class CookedRawComplete(blockId: Long, files: List[File]) extends NIHActorMessage

// FIXME: dedup this
case class ProjectionGetBlock(descriptor: ProjectionDescriptor, id: Option[Long], columns: Set[ColumnDescriptor])

trait StorageReader {
  def blockId: Long
  def slice: Slice
  def size: Long
  def close: Unit // Better name for this?
}

object RawReader {
  def apply(blockId: Long, file: File): StorageReader = null
}

trait CookedReader extends StorageReader {
  def files: List[File]
}

object CookedReader {
  def apply(blockId: Long, files: List[File]): CookedReader = null
}

trait RawWriter extends StorageReader {
  def write(rows: Seq[ProjectionInsert.Row]): Unit = {}
}

object RawWriter {
  def apply(blockId: Long, file: File): RawWriter = null
}

/**
  *  Projection for NIH DB files
  *
  * @param cookThreshold The threshold, in bytes, of raw data for cooking a raw store file
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

  private[this] var currentState: BlockState = {
    val currentLog = RawWriter(txLog.currentBlockId, rawFileFor(txLog.currentBlockId))

    val pendingCooks = txLog.pendingCookIds.map {
      id => (id, RawReader(id, rawFileFor(id)))
    }.toMap

    // Restore the cooked map
    val cooked = CookedMap(cookedMapFile, cookedDir)

    BlockState(cooked, pendingCooks, currentLog)
  }

  private[this] var currentBlocks: SortedMap[Long, StorageReader] = computeBlockMap(currentState)

  // Re-fire any restored pending cooks
  currentState.pending.foreach {
    case (blockId, reader) => cooker ! CookRawRequest(blockId, reader.slice)
  }

  override def postStop() = {
    IO { currentBlocks.foreach (_._2.close) }.ensuring { IO { txLog.close } }.except {
      case t: Throwable => IO { logger.error("Error during close", t) }
    }.unsafePerformIO

    workLock.release
  }

  private def rawFileFor(seq: Long) = new File(rawDir, "%06x.raw".format(seq))

  private def computeBlockMap(current: BlockState) = {
    val allBlocks: List[StorageReader] = (current.cooked ++ current.pending.values :+ current.rawLog).sortBy(_.blockId)
    SortedMap(allBlocks.map { r => r.blockId -> r }.toSeq: _*)
  }

  override def receive = {
    case CookedRawComplete(blockId, files) =>
      // This could be a replacement for an existing blockId, so we
      // ned to remove/close any existing cooked block with the same
      // ID
      currentState = currentState.copy(
        cooked = CookedReader(blockId, files) :: currentState.cooked.filterNot(_.blockId == blockId),
        pending = currentState.pending - blockId
      )
      currentBlocks = computeBlockMap(currentState)
      CookedMap.write(cookedMapFile, currentState.cooked)
      txLog.completeCook(blockId)

    case ProjectionInsert(_, rows) =>
      currentState.rawLog.write(rows)

      if (currentState.rawLog.size >= cookThreshold) {
        val toCook = currentState.rawLog
        val newRaw = RawWriter(toCook.blockId + 1, rawFileFor(toCook.blockId + 1))

        currentState = currentState.copy(pending = currentState.pending + (toCook.blockId -> toCook), rawLog = newRaw)
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

object CookedMap extends Logging {
  def apply(mapFile: File, cookedBaseDir: File): List[CookedReader] = {
    val rawMap: Validation[Extractor.Error, Map[String, List[String]]] =
      JParser.parseFromFile(mapFile).bimap(Extractor.Thrown(_), x => x).flatMap { jvMap =>
        jvMap.validated[Map[String,List[String]]]
      }

    rawMap match {
      case Success(m) =>
        m.map {
          case (key, values) =>
            val files = values map { filename => new File(cookedBaseDir, filename) }

            try {
              CookedReader(key.toLong, files)
            } catch {
              case nfe: NumberFormatException => throw new NumberFormatException("Invalid block ID: " + key)
            }
        }.toList.sortBy(_.blockId)

      case Failure(error) =>
        logger.error("Error restoring cooked map file: " + error.message)
        error.die
    }
  }

  def write(mapFile: File, cookedMap: List[CookedReader]) = {
    val jv = JObject(cookedMap.map {
      reader => (reader.blockId.toString, JArray(reader.files.map { f => JString(f.getName) }))
    })

    IOUtils.safeWriteToFile(jv.renderCompact, mapFile)
  }
}
