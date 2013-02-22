package com.precog.common

import com.precog.common.json._
import com.precog.util.VectorClock

import blueeyes.json._
import blueeyes.json.serialization.{ Extractor, Decomposer }
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.Extractor._

import com.weiglewilczek.slf4s._

import java.util.concurrent.atomic.AtomicInteger

import shapeless._
import scalaz._
import scalaz.syntax.apply._
import scalaz.syntax.plus._

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

object IdSequenceBlock {
  implicit val iso = Iso.hlist(IdSequenceBlock.apply _ , IdSequenceBlock.unapply _)
  val schemaV1 = "producerId" :: "firstSequenceId" :: "lastSequenceId" :: HNil
  val extractorPreV = extractorV[IdSequenceBlock](schemaV1, None)
  val (decomposerV1, extractorV1) = serializationV[IdSequenceBlock](schemaV1, Some("1.0"))
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
  val (decomposerV1, extractorV1) = serializationV[EventRelayState](schemaV1, Some("1.0"))
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
}

object YggCheckpoint {
  val Empty = YggCheckpoint(0, VectorClock.empty)

  sealed trait LoadError
  case class CheckpointParseError(message: String) extends LoadError
  case class ShardCheckpointMissing(shardId: String) extends LoadError

  implicit val iso = Iso.hlist(YggCheckpoint.apply _ , YggCheckpoint.unapply _)
  val schemaV1 = "offset" :: "messageClock" :: HNil

  val extractorPreV = extractorV[YggCheckpoint](schemaV1, None)
  val (decomposerV1, extractorV1) = serializationV[YggCheckpoint](schemaV1, Some("1.0"))

  implicit val decomposer = decomposerV1
  implicit val extractor = extractorV1 <+> extractorPreV

  implicit val ordering = scala.math.Ordering.by((_: YggCheckpoint).offset)
}

case class ServiceUID(systemId: String, hostId: String, serviceId: String)
