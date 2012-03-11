package com.precog.common
package util 

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

  def renewEventRelayState(agent: String, offset: Long, producerId: Int, blockSize: Int): Validation[Error, EventRelayState]
  def saveEventRelayState(agent: String, state: EventRelayState): Validation[Error, EventRelayState] 

  def loadYggCheckpoint(shard: String): Validation[Error, YggCheckpoint]
  def saveYggCheckpoint(shard: String, checkpoint: YggCheckpoint): Unit

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
  val empty = YggCheckpoint(0, VectorClock.empty)
}

case class VectorClock(map: Map[Int, Int]) {   
  def get(id: Int): Option[Int] = map.get(id)  
  def hasId(id: Int): Boolean = map.contains(id)
  def update(id: Int, sequence: Int) = 
    if(map.get(id) forall { sequence > _ }) {
      VectorClock(map + (id -> sequence))
    } else {
      this 
    }
  def isLowerBoundOf(other: VectorClock): Boolean = map forall { 
    case (prodId, maxSeqId) => other.get(prodId).map( _ >= maxSeqId).getOrElse(true)
  }
}

trait VectorClockSerialization {
  implicit val VectorClockDecomposer: Decomposer[VectorClock] = new Decomposer[VectorClock] {
    override def decompose(clock: VectorClock): JValue = clock.map.serialize 
  }

  implicit val VectorClockExtractor: Extractor[VectorClock] = new Extractor[VectorClock] with ValidatedExtraction[VectorClock] {
    override def validated(obj: JValue): Validation[Error, VectorClock] = 
      (obj.validated[Map[Int, Int]]).map(VectorClock(_))
  }
}

object VectorClock extends VectorClockSerialization {
  def empty = apply(Map.empty)
}

class ZookeeperSystemCoordination(private val zkc: ZkClient, 
                      basePaths: Seq[String],
                      prefix: String) extends SystemCoordination with Logging {

  lazy val initialSequenceId = 0

  lazy val active = "active"
  lazy val delimeter = "/"

  lazy val producerIdBasePaths = List("producer", "id")
  lazy val relayAgentBasePaths = List("relay_agent")
  lazy val shardCheckpointBasePaths = List("shard", "checkpoint")

  lazy val basePath = delimeter + basePaths.mkString(delimeter)  

  private def makeBase(elements: List[String]) = basePath + delimeter + elements.mkString(delimeter)

  lazy val producerIdBase = makeBase(producerIdBasePaths)
  lazy val producerIdPath = producerIdBase + delimeter + prefix  

  lazy val relayAgentBase = makeBase(relayAgentBasePaths)
  
  lazy val shardCheckpointBase = makeBase(shardCheckpointBasePaths) 


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

  val defaultRetries = 20
  val defaultDelay = 1000

  private def acquireActivePath(base: String, retries: Int = defaultRetries, delay: Int = defaultDelay): Validation[Error, Unit] = {
    val activePath = base + delimeter + active
    if(retries < 0) {
      Failure(Invalid("Unable to acquire relay agent lock"))
    } else {
      if(!zkc.exists(activePath)) {
        if(!zkc.exists(base)) { 
          zkc.createPersistent(base, true)
        }
        zkc.createEphemeral(activePath)
        Success(())
      } else {
        Thread.sleep(delay)
        logger.debug("Active path [%s] already registered, retrying in case of stale registration.".format(base))
        acquireActivePath(base, retries - 1, delay)
      }
    }
  }

  def registerRelayAgent(agent: String, blockSize: Int): Validation[Error, EventRelayState] = {
    
    val agentPath = relayAgentPath(agent)

    acquireActivePath(agentPath) flatMap { _ =>
      val bytes = zkc.readData(agentPath).asInstanceOf[Array[Byte]]
      if(bytes != null && bytes.length != 0) {
        val jvalue = JsonParser.parse(new String(bytes)) //TODO: Specify an encoding here and wherever this is written
        val state = jvalue.validated[EventRelayState]
        logger.debug("%s: RESTORED".format(state))
        state
      } else {
        val producerId = acquireProducerId()
        val block = acquireIdSequenceBlock(producerId, blockSize)
        val initialState = EventRelayState(0, block.firstSequenceId, block)
        zkc.updateDataSerialized(relayAgentPath(agent), new DataUpdater[Array[Byte]] {
          def update(cur: Array[Byte]): Array[Byte] = jvalueToBytes(initialState.serialize)
        })
        logger.debug("%s: NEW".format(initialState))
        Success(initialState)
      }
    }
  }

  private def relayAgentPath(agent: String): String = relayAgentBase + delimeter + agent 
  private def relayAgentActivePath(agent: String): String = relayAgentPath(agent) + delimeter + active

  def unregisterRelayAgent(agent: String, state: EventRelayState): Unit = {
    // update offset
    saveEventRelayState(agent, state)
    // remove relay agent active status
    zkc.delete(relayAgentActivePath(agent))
  }
 
  def renewEventRelayState(agent: String, offset: Long, producerId: Int, blockSize: Int): Validation[Error, EventRelayState] = {
    val block = acquireIdSequenceBlock(producerId, blockSize)
    val newState = EventRelayState(offset, block.firstSequenceId, block)
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

  def loadYggCheckpoint(shard: String): Validation[Error, YggCheckpoint] = {
    val checkpointPath = shardCheckpointPath(shard)

    acquireActivePath(checkpointPath) flatMap { _ =>
      val bytes = zkc.readData(checkpointPath).asInstanceOf[Array[Byte]]
      if(bytes != null && bytes.length != 0) {
        val jvalue = JsonParser.parse(new String(bytes))
        val checkpoint = jvalue.validated[YggCheckpoint]
        logger.debug("%s: RESTORED".format(checkpoint))
        checkpoint 
      } else {
        val initialCheckpoint = YggCheckpoint.empty 
        zkc.updateDataSerialized(shardCheckpointPath(shard), new DataUpdater[Array[Byte]] {
          def update(cur: Array[Byte]): Array[Byte] = jvalueToBytes(initialCheckpoint.serialize)
        })
        logger.debug("%s: NEW".format(initialCheckpoint))
        Success(initialCheckpoint)
      }
    }
  }

  def shardCheckpointExists(shard: String): Boolean = zkc.exists(shardCheckpointPath(shard))

  private def shardCheckpointPath(shard: String): String = shardCheckpointBase + delimeter + shard
  private def shardCheckpointActivePath(shard: String): String = shardCheckpointPath(shard) + delimeter + active 

  def saveYggCheckpoint(shard: String, checkpoint: YggCheckpoint): Unit = {
    zkc.updateDataSerialized(shardCheckpointPath(shard), new DataUpdater[Array[Byte]] {
      def update(cur: Array[Byte]): Array[Byte] = jvalueToBytes(checkpoint.serialize)
    })
    logger.debug("%s: SAVE".format(checkpoint))
  }
  
  def relayAgentExists(agent: String) = zkc.exists(relayAgentPath(agent))

  def relayAgentActive(agent: String) = zkc.exists(relayAgentActivePath(agent))

  def close() = zkc.close()
}

object ZookeeperSystemCoordination {

  def apply(zkHosts: String, basePaths: Seq[String], prefix: String) = {
    val zkc = new ZkClient(zkHosts)
    new ZookeeperSystemCoordination(zkc, basePaths, prefix)
  }

  val prefix = "prodId-"
  val paths = List("com", "precog", "ingest", "v1")

  val testHosts = "localhost:2181"
  def testZookeeperSystemCoordination(hosts: String = testHosts) = ZookeeperSystemCoordination(hosts, "test" :: paths, prefix)

  val prodHosts = "localhost:2181"
  def prodZookeeperSystemCoordination(hosts: String = prodHosts) = ZookeeperSystemCoordination(hosts, paths, prefix)
}
