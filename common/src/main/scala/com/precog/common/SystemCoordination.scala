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
package com.precog.common

import com.precog.util.VectorClock

import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.Versioned._

import com.weiglewilczek.slf4s._

import java.util.concurrent.atomic.AtomicInteger

import shapeless._
import scalaz._
import scalaz.syntax.apply._
import scalaz.syntax.plus._

trait CheckpointCoordination {
  def loadYggCheckpoint(bifrost: String): Option[Validation[Error, YggCheckpoint]]
  def saveYggCheckpoint(bifrost: String, checkpoint: YggCheckpoint): Unit
}

object CheckpointCoordination {
  object Noop extends CheckpointCoordination {
    def loadYggCheckpoint(bifrost: String): Option[Validation[Error, YggCheckpoint]] = None
    def saveYggCheckpoint(bifrost: String, checkpoint: YggCheckpoint): Unit = ()
  }
}

trait SystemCoordination extends CheckpointCoordination {
  def registerRelayAgent(agent: String, blockSize: Int): Validation[Error, EventRelayState]
  def unregisterRelayAgent(agent: String, state: EventRelayState): Unit

  def renewEventRelayState(agent: String, offset: Long, producerId: Int, blockSize: Int): Validation[Error, EventRelayState]
  def saveEventRelayState(agent: String, state: EventRelayState): Validation[Error, EventRelayState]

  def close(): Unit
}

sealed trait IdSequence {
  def isEmpty(): Boolean
  def next(): (Int, Int)
}

case object EmptyIdSequence extends IdSequence {
  def isEmpty(): Boolean = true
  def next() = sys.error("No ids available from empty id sequence block")
}


case class IdSequenceBlock(producerId: Int, firstSequenceId: Int, lastSequenceId: Int) extends IdSequence {
  private val currentSequenceId = new AtomicInteger(firstSequenceId)

  def isEmpty() = currentSequenceId.get > lastSequenceId

  def next() = {
    val sequenceId = currentSequenceId.getAndIncrement
    if(sequenceId > lastSequenceId) sys.error("Id sequence block is exhausted no more ids available.")
    (producerId, sequenceId)
  }
}

object IdSequenceBlock {
  implicit val iso = Iso.hlist(IdSequenceBlock.apply _ , IdSequenceBlock.unapply _)
  val schemaV1 = "producerId" :: "firstSequenceId" :: "lastSequenceId" :: HNil
  val extractorPreV = extractorV[IdSequenceBlock](schemaV1, None)
  val (decomposerV1, extractorV1) = serializationV[IdSequenceBlock](schemaV1, Some("1.0".v))
  implicit val decomposer = decomposerV1
  implicit val extractor = extractorV1 <+> extractorPreV
}

case class EventRelayState(offset: Long, nextSequenceId: Int, idSequenceBlock: IdSequenceBlock) {
  override def toString() = "EventRelayState[ offset: %d prodId: %d seqId: %d in [%d,%d] ]".format(
    offset, idSequenceBlock.producerId, nextSequenceId, idSequenceBlock.firstSequenceId, idSequenceBlock.lastSequenceId
  )
}

object EventRelayState {
  implicit val iso = Iso.hlist(EventRelayState.apply _ , EventRelayState.unapply _)
  val schemaV1 = "offset" :: "nextSequenceId" :: "idSequenceBlock" :: HNil
  val extractorPreV = extractorV[EventRelayState](schemaV1, None)
  val (decomposerV1, extractorV1) = serializationV[EventRelayState](schemaV1, Some("1.0".v))
  implicit val decomposer = decomposerV1
  implicit val extractor = extractorV1 <+> extractorPreV
}

case class ProducerState(lastSequenceId: Int)

object ProducerState {
  implicit val ProducerStateDecomposer = implicitly[Decomposer[Int]].contramap((_:ProducerState).lastSequenceId)
  implicit val ProducerStateExtractor = implicitly[Extractor[Int]].map(ProducerState.apply _)
}

case class YggCheckpoint(offset: Long, messageClock: VectorClock) {
  def update(newOffset: Long, newPid: Int, newSid: Int) = this.copy(offset max newOffset, messageClock.update(newPid, newSid))
  def skipTo(newOffset: Long) = this.copy(offset max newOffset, messageClock)
}

object YggCheckpoint {
  val Empty = YggCheckpoint(0, VectorClock.empty)

  sealed trait LoadError
  case class CheckpointParseError(message: String) extends LoadError
  case class ShardCheckpointMissing(shardId: String) extends LoadError

  implicit val iso = Iso.hlist(YggCheckpoint.apply _ , YggCheckpoint.unapply _)
  val schemaV1 = "offset" :: "messageClock" :: HNil

  val extractorPreV = extractorV[YggCheckpoint](schemaV1, None)
  val (decomposerV1, extractorV1) = serializationV[YggCheckpoint](schemaV1, Some("1.0".v))

  implicit val decomposer = decomposerV1
  implicit val extractor = extractorV1 <+> extractorPreV

  implicit val ordering = scala.math.Ordering.by((_: YggCheckpoint).offset)
}

case class ServiceUID(systemId: String, hostId: String, serviceId: String)
