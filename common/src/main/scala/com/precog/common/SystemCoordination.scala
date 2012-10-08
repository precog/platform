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

import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser
import blueeyes.json.Printer 

import blueeyes.json.serialization.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import com.weiglewilczek.slf4s._

import java.util.concurrent.atomic.AtomicInteger

import scalaz._
import scalaz.syntax.apply._

trait CheckpointCoordination {
  def loadYggCheckpoint(shard: String): Option[Validation[Error, YggCheckpoint]]
  def saveYggCheckpoint(shard: String, checkpoint: YggCheckpoint): Unit
}

object CheckpointCoordination {
  object Noop extends CheckpointCoordination {
    def loadYggCheckpoint(shard: String): Option[Validation[Error, YggCheckpoint]] = None
    def saveYggCheckpoint(shard: String, checkpoint: YggCheckpoint): Unit = ()
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

trait IdSequenceBlockSerialization {
  implicit val IdSequenceBlockDecomposer: Decomposer[IdSequenceBlock] = new Decomposer[IdSequenceBlock] {
    override def decompose(block: IdSequenceBlock): JValue = JObject(List(
      JField("producerId", block.producerId),
      JField("firstSequenceId", block.firstSequenceId),
      JField("lastSequenceId", block.lastSequenceId)
    ))
  }

  implicit val IdSequenceBlockExtractor: Extractor[IdSequenceBlock] = new Extractor[IdSequenceBlock] with ValidatedExtraction[IdSequenceBlock] {
    override def validated(obj: JValue): Validation[Error, IdSequenceBlock] = 
      ((obj \ "producerId").validated[Int] |@|
       (obj \ "firstSequenceId").validated[Int] |@|
       (obj \ "lastSequenceId").validated[Int]).apply(IdSequenceBlock(_,_,_))
  }
}

object IdSequenceBlock extends IdSequenceBlockSerialization


case class EventRelayState(offset: Long, nextSequenceId: Int, idSequenceBlock: IdSequenceBlock) {
  override def toString() = "EventRelayState[ offset: %d prodId: %d seqId: %d in [%d,%d] ]".format(
    offset, idSequenceBlock.producerId, nextSequenceId, idSequenceBlock.firstSequenceId, idSequenceBlock.lastSequenceId
  )
}

trait EventRelayStateSerialization {
  implicit val EventRelayStateDecomposer: Decomposer[EventRelayState] = new Decomposer[EventRelayState] {
    override def decompose(state: EventRelayState): JValue = JObject(List(
      JField("offset", state.offset),
      JField("nextSequenceId", state.nextSequenceId),
      JField("idSequenceBlock", state.idSequenceBlock.serialize)
    ))
  }

  implicit val EventRelayStateExtractor: Extractor[EventRelayState] = new Extractor[EventRelayState] with ValidatedExtraction[EventRelayState] {
    override def validated(obj: JValue): Validation[Error, EventRelayState] = 
      ((obj \ "offset").validated[Long] |@|
       (obj \ "nextSequenceId").validated[Int] |@|
       (obj \ "idSequenceBlock").validated[IdSequenceBlock]).apply(EventRelayState(_,_,_))
  }
}

object EventRelayState extends EventRelayStateSerialization


case class ProducerState(lastSequenceId: Int)

trait ProducerStateSerialization {
  implicit val ProducerStateDecomposer: Decomposer[ProducerState] = new Decomposer[ProducerState] {
    override def decompose(state: ProducerState): JValue = JNum(state.lastSequenceId)
  }

  implicit val ProducerStateExtractor: Extractor[ProducerState] = new Extractor[ProducerState] with ValidatedExtraction[ProducerState] {
    override def validated(obj: JValue): Validation[Error, ProducerState] = obj match {
      case jint @ JNum(_) => jint.validated[Int] map { id => ProducerState(id) }
      case _              => Failure(Invalid("Invalid producer state: " + obj))
    }   
  }
}

object ProducerState extends ProducerStateSerialization


case class YggCheckpoint(offset: Long, messageClock: VectorClock)

trait YggCheckpointSerialization {
  implicit val YggCheckpointDecomposer: Decomposer[YggCheckpoint] = new Decomposer[YggCheckpoint] {
    override def decompose(checkpoint: YggCheckpoint): JValue = JObject(List(
      JField("offset", checkpoint.offset),
      JField("messageClock", checkpoint.messageClock)
    ))
  }

  implicit val YggCheckpointExtractor: Extractor[YggCheckpoint] = new Extractor[YggCheckpoint] with ValidatedExtraction[YggCheckpoint] {
    override def validated(obj: JValue): Validation[Error, YggCheckpoint] = 
      ((obj \ "offset").validated[Long] |@|
       (obj \ "messageClock").validated[VectorClock]).apply(YggCheckpoint(_,_))
  }
}

object YggCheckpoint extends YggCheckpointSerialization {
  import scala.math.Ordering
  implicit val ordering: Ordering[YggCheckpoint] = Ordering.by((_: YggCheckpoint).offset)

  val Empty = YggCheckpoint(0, VectorClock.empty)

  sealed trait LoadError 
  case class CheckpointParseError(message: String) extends LoadError
  case class ShardCheckpointMissing(shardId: String) extends LoadError

}

case class ServiceUID(systemId: String, hostId: String, serviceId: String)


