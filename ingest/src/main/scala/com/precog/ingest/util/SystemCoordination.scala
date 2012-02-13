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
package com.precog.ingest.util 

import java.util.ArrayList
import java.util.concurrent.atomic.AtomicInteger
  
import blueeyes.json.JsonAST._
import blueeyes.json.JPath
import blueeyes.json.JsonParser
import blueeyes.json.Printer 

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import scalaz._
import Scalaz._

import com.weiglewilczek.slf4s._

import org.I0Itec.zkclient.ZkClient 
import org.I0Itec.zkclient.DataUpdater

trait SystemCoordination {

  def registerRelayAgent(agent: String, blockSize: Int): Validation[Error, EventRelayState]
  def unregisterRelayAgent(agent: String, state: EventRelayState): Unit

  def renewEventRelayState(agent: String, state: EventRelayState, blockSize: Int): Validation[Error, EventRelayState]
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

case class IdSequenceBlock(producerId: Int, firstSequenceId: Int, lastSequenceId: Int) extends IdSequence {    private val currentSequenceId = new AtomicInteger(firstSequenceId)

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
    override def decompose(state: ProducerState): JValue = JInt(state.lastSequenceId)
  }

  implicit val ProducerStateExtractor: Extractor[ProducerState] = new Extractor[ProducerState] with ValidatedExtraction[ProducerState] {
    override def validated(obj: JValue): Validation[Error, ProducerState] = obj match {
      case jint @ JInt(_) => jint.validated[Int] map { id => ProducerState(id) }
      case _              => Failure(Invalid("Invalid producer state: " + obj))
    }   
  }
}

object ProducerState extends ProducerStateSerialization

class ZookeeperSystemCoordination(zkHosts: String, 
                      basePaths: Seq[String],
                      prefix: String) extends SystemCoordination with Logging {

  lazy val initialSequenceId = 0

  lazy val active = "active"
  lazy val delimeter = "/"

  lazy val producerIdBasePaths = List("producer", "id")
  lazy val relayAgentBasePaths = List("relay_agent")

  lazy val basePath = delimeter + basePaths.mkString(delimeter)  

  lazy val producerIdBase = basePath + delimeter + producerIdBasePaths.mkString(delimeter)
  lazy val producerIdPath = producerIdBase + delimeter + prefix  

  lazy val relayAgentBase = basePath + delimeter + relayAgentBasePaths.mkString(delimeter)

  private val zkc = new ZkClient(zkHosts)

  def jvalueToBytes(jval: JValue): Array[Byte] = Printer.compact(Printer.render(jval)).getBytes

  def acquireProducerId(): Int = {
    val data = jvalueToBytes(ProducerState(initialSequenceId).serialize)
    createPersistentSequential(producerIdBase, prefix, data) 
  }

  private def createPersistentSequential(path: String, prefix: String, data: Array[Byte]): Int = {
    zkc.createPersistent(path, true)
    val actualPath = zkc.createPersistentSequential(path + delimeter + prefix, data)
    actualPath.substring(actualPath.length - 10).toInt
  }

  def registerProducerId(preferredProducerId: Option[Int] = None) = {
    val producerId = preferredProducerId filter { prodId => !producerIdInUse(prodId) } getOrElse acquireProducerId
    zkc.createEphemeral(producerActivePath(producerId))
    producerId
  }

  private def producerPath(producerId: Int): String = producerIdBase + delimeter + prefix + "%010d".format(producerId)
  private def producerActivePath(producerId: Int): String = producerPath(producerId) + delimeter + active

  def unregisterProducerId(producerId: Int) {
    zkc.delete(producerActivePath(producerId))
  }

  def producerIdInUse(producerId: Int): Boolean = {
    zkc.exists(producerActivePath(producerId))
  }

  def acquireIdSequenceBlock(producerId: Int, blockSize: Int): IdSequenceBlock = {

    val updater = new DataUpdater[Array[Byte]] {

      private var newState: ProducerState = null

      def newProducerState(): Option[ProducerState] = 
        if(newState == null) None else Some(newState)

      def update(cur: Array[Byte]): Array[Byte] = {
        JsonParser.parse(new String(cur)).validated[ProducerState] map { ps =>
          newState = ProducerState(ps.lastSequenceId + blockSize)
          jvalueToBytes(newState.serialize)
        } getOrElse(cur)
      }
    }

    zkc.updateDataSerialized(producerPath(producerId), updater) 

    updater.newProducerState.map {
      case ProducerState(next) => IdSequenceBlock(producerId, next - blockSize + 1, next) 
    }.getOrElse(sys.error("Unable to get new producer sequence block"))
  }

  def registerRelayAgent(agent: String, blockSize: Int): Validation[Error, EventRelayState] = {
    val agentPath = relayAgentPath(agent)
    val state = if(relayAgentExists(agent)) {
      val bytes = zkc.readData(agentPath).asInstanceOf[Array[Byte]]
      val jvalue = JsonParser.parse(new String(bytes))
      val state = jvalue.validated[EventRelayState]
      logger.debug("%s: RESTORED".format(state))
      state
    } else {
      val producerId = acquireProducerId()
      val block = acquireIdSequenceBlock(producerId, blockSize)
      val initialState = EventRelayState(0, block.firstSequenceId, block)
      zkc.createPersistent(relayAgentBase, true)
      zkc.createPersistent(agentPath, jvalueToBytes(initialState.serialize))
      logger.debug("%s: NEW".format(initialState))
      Success(initialState)
    }
    state.foreach { _ =>
      zkc.createEphemeral(relayAgentActivePath(agent))
    }
    state
  }

  private def relayAgentPath(agent: String): String = relayAgentBase + delimeter + agent 
  private def relayAgentActivePath(agent: String): String = relayAgentPath(agent) + delimeter + active

  def unregisterRelayAgent(agent: String, state: EventRelayState): Unit = {
    // update offset
    saveEventRelayState(agent, state)
    // remove relay agent active status
    zkc.delete(relayAgentActivePath(agent))
  }
 
  def renewEventRelayState(agent: String, state: EventRelayState, blockSize: Int): Validation[Error, EventRelayState] = {
    val block = acquireIdSequenceBlock(state.idSequenceBlock.producerId, blockSize)
    val newState = state.copy(state.offset, block.firstSequenceId, block)
    logger.debug("%s: RENEWAL".format(newState))
    saveEventRelayState(agent, newState)
  }

  def saveEventRelayState(agent: String, state: EventRelayState): Validation[Error, EventRelayState] = {
    zkc.updateDataSerialized(relayAgentPath(agent), new DataUpdater[Array[Byte]] {
      def update(cur: Array[Byte]): Array[Byte] = jvalueToBytes(state.serialize)
    })
    logger.debug("%s: SAVE".format(state))
    Success(state)
  }

  def relayAgentExists(agent: String): Boolean = {
    zkc.exists(relayAgentPath(agent))
  }

  def relayAgentActive(agent: String): Boolean = {
    zkc.exists(relayAgentActivePath(agent))
  }

  def close() {
    zkc.close()
  }
}

object ZookeeperSystemCoordination {

  val prefix = "prodId-"
  val paths = List("com", "precog", "ingest", "v1")

  val testHosts = "localhost:2181"
  def testZookeeperSystemCoordination(hosts: String = testHosts) = new ZookeeperSystemCoordination(hosts, "test" :: paths, prefix)

  val prodHosts = "localhost:2181"
  def prodZookeeperSystemCoordination(hosts: String = prodHosts) = new ZookeeperSystemCoordination(hosts, paths, prefix)
}
