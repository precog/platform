package com.precog.common
package util 

import java.util.concurrent.atomic.AtomicInteger
import java.net.InetAddress
  
import blueeyes.json.JsonAST._
import blueeyes.json.JsonParser
import blueeyes.json.Printer 

import blueeyes.json.xschema.{ ValidatedExtraction, Extractor, Decomposer }
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import com.weiglewilczek.slf4s._

import org.streum.configrity.Configuration

import org.I0Itec.zkclient.ZkClient 
import org.I0Itec.zkclient.DataUpdater

import scalaz._
import scalaz.syntax.apply._

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

case class ServiceUID(systemId: String, hostId: String, serviceId: String)

object ZookeeperSystemCoordination {
  val defaultRetries = 20
  val defaultDelay = 1000

  val initialSequenceId = 0

  val active = "active"
  val delimeter = "/"

  val producerIdBasePaths = List("ingest", "producer_id")
  val relayAgentBasePaths = List("ingest", "relay_agent")
  val shardCheckpointBasePaths = List("shard", "checkpoint")

  def toNodeData(jval: JValue): Array[Byte] = Printer.compact(Printer.render(jval)).getBytes("UTF-8")
  def fromNodeData(bytes: Array[Byte]): JValue = JsonParser.parse(new String(bytes))

  def apply(zkHosts: String, uid: ServiceUID) = {
    val zkc = new ZkClient(zkHosts)
    new ZookeeperSystemCoordination(zkc, uid)
  }

  def extractServiceUID(config: Configuration): ServiceUID = {
    val systemId = config[String]("systemId", "test")
    val hostId = config[String]("hostId", InetAddress.getLocalHost.getHostName)
    val serviceId = config[String]("serviceId", System.getProperty("precog.serviceId", ""))
    ServiceUID(systemId, hostId, serviceId)
  }
}

class ZookeeperSystemCoordination(private val zkc: ZkClient, uid: ServiceUID) extends SystemCoordination with Logging {
  import ZookeeperSystemCoordination._

  lazy val basePath = delimeter + "precog-" + uid.systemId 

  lazy val producerIdBase = makeBase(producerIdBasePaths)
  lazy val producerIdPath = producerIdBase + delimeter + uid.hostId + uid.serviceId 

  lazy val relayAgentBase = makeBase(relayAgentBasePaths)
  
  lazy val shardCheckpointBase = makeBase(shardCheckpointBasePaths) 

  private def makeBase(elements: List[String]) = basePath + delimeter + elements.mkString(delimeter)
  private def producerPath(producerId: Int): String = producerIdPath + "%010d".format(producerId)
  private def producerActivePath(producerId: Int): String = producerPath(producerId) + delimeter + active

  def acquireProducerId(): Int = {
    zkc.createPersistent(producerIdBase, true /* create parents */)

    // sequential nodes created by zookeeper will be suffixed with a new 10-character
    // integer that represents a montonic increase of the underlying counter.
    val data = toNodeData(ProducerState(initialSequenceId).serialize)
    val createdPath = zkc.createPersistentSequential(producerIdPath,  data)
    createdPath.substring(createdPath.length - 10).toInt
  }

  def registerProducerId(preferredProducerId: Option[Int] = None) = {
    val producerId = preferredProducerId filter { prodId => !producerIdInUse(prodId) } getOrElse acquireProducerId
    zkc.createEphemeral(producerActivePath(producerId))
    producerId
  }

  def unregisterProducerId(producerId: Int) {
    zkc.delete(producerActivePath(producerId))
  }

  def producerIdInUse(producerId: Int): Boolean = {
    zkc.exists(producerActivePath(producerId))
  }

  /**
   * An implementation of the DataUpdater interface that increments the sequence ID of a producer ID by 
   * the specified block size, and makes the updated producer state available for subsequent reads
   */
  class BlockUpdater(blockSize: Int) extends DataUpdater[Array[Byte]] {
    private var newState: ProducerState = _

    def newProducerState(): Option[ProducerState] = Option(newState)

    def update(cur: Array[Byte]): Array[Byte] = {
      fromNodeData(cur).validated[ProducerState] match { 
        case Success(ps) =>
          this.newState = ProducerState(ps.lastSequenceId + blockSize)
          toNodeData(newState.serialize)

        case Failure(_) => 
          cur
      }
    }
  }

  def acquireIdSequenceBlock(producerId: Int, blockSize: Int): IdSequenceBlock = {
    val updater = new BlockUpdater(blockSize)
    zkc.updateDataSerialized(producerPath(producerId), updater) 

    updater.newProducerState match {
      case Some(ProducerState(next)) => 
        // updating the producer state advances the state counter by blockSize, meaning 
        // that the start start of the block is derived by subtraction
        IdSequenceBlock(producerId, next - blockSize + 1, next) 

      case None => 
        sys.error("Unable to get new producer sequence block")
    }
  }

  private def acquireActivePath(base: String, retries: Int = defaultRetries, delay: Int = defaultDelay): Validation[Error, Unit] = {
    val activePath = base + delimeter + active
    if (retries < 0) {
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
      if (bytes != null && bytes.length != 0) {
        val state = fromNodeData(bytes).validated[EventRelayState]
        logger.debug("%s: RESTORED".format(state))
        state
      } else {
        val producerId = acquireProducerId()
        val block = acquireIdSequenceBlock(producerId, blockSize)
        val initialState = EventRelayState(0, block.firstSequenceId, block)
        zkc.updateDataSerialized(relayAgentPath(agent), new DataUpdater[Array[Byte]] {
          def update(cur: Array[Byte]): Array[Byte] = toNodeData(initialState.serialize)
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
      def update(cur: Array[Byte]): Array[Byte] = toNodeData(state.serialize)
    })
    logger.debug("%s: SAVE".format(state))
    Success(state)
  }

  def loadYggCheckpoint(shard: String): Validation[Error, YggCheckpoint] = {
    val checkpointPath = shardCheckpointPath(shard)

    acquireActivePath(checkpointPath) flatMap { _ =>
      val bytes = zkc.readData(checkpointPath).asInstanceOf[Array[Byte]]
      if (bytes != null && bytes.length != 0) {
        val checkpoint = fromNodeData(bytes).validated[YggCheckpoint]
        logger.debug("%s: RESTORED".format(checkpoint))
        checkpoint 
      } else {
        val initialCheckpoint = YggCheckpoint.empty 
        zkc.updateDataSerialized(shardCheckpointPath(shard), new DataUpdater[Array[Byte]] {
          def update(cur: Array[Byte]): Array[Byte] = toNodeData(initialCheckpoint.serialize)
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
      def update(cur: Array[Byte]): Array[Byte] = toNodeData(checkpoint.serialize)
    })
    logger.debug("%s: SAVE".format(checkpoint))
  }
  
  def relayAgentExists(agent: String) = zkc.exists(relayAgentPath(agent))

  def relayAgentActive(agent: String) = zkc.exists(relayAgentActivePath(agent))

  def close() = zkc.close()
}
