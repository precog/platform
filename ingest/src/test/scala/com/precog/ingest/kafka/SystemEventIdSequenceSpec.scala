package com.precog.ingest.kafka

import org.specs2.mutable._
import com.precog.ingest.SystemEventIdSequence
import com.precog.common.{IdSequenceBlock, EventRelayState, YggCheckpoint, SystemCoordination}
import scalaz.Validation
import scalaz.syntax.validation._
import blueeyes.json.serialization.Extractor.Error
import com.precog.ingest.SystemEventIdSequence.UnableToRetrieveRelayAgentState

class SystemEventIdSequenceSpec extends Specification {
  trait MockCoordination extends SystemCoordination { // Use a zookeeper?
    def loadYggCheckpoint(shard: String): Option[Validation[Error, YggCheckpoint]] = None
    def saveYggCheckpoint(shard: String, checkpoint: YggCheckpoint) {}
    def unregisterRelayAgent(agent: String, state: EventRelayState) {}
    def renewEventRelayState(agent: String, offset: Long, producerId: Int, blockSize: Int) = Error.invalid("bad relay").fail[EventRelayState]
    def saveEventRelayState(agent: String, state: EventRelayState) = Error.invalid("bad relay").fail[EventRelayState]
    def close() {}
  }

  "the system event id sequence" should {
    "reliably obtain a starting state" in {
      val coordination = new MockCoordination {
        def registerRelayAgent(agent: String, blockSize: Int) = EventRelayState(0, 0, IdSequenceBlock(0, 0, 0)).success[Error]
      }
      val eventIdSequence = SystemEventIdSequence("test", coordination)

      eventIdSequence.getLastOffset() must be_>=(0L)
    }

    "not allow one to obtain a sequence with an invalid starting state" in {
      val coordination = new MockCoordination {
        def registerRelayAgent(agent: String, blockSize: Int) = Error.invalid("bad relay").fail[EventRelayState]
      }

      SystemEventIdSequence("test", coordination) must throwAn[UnableToRetrieveRelayAgentState]
    }

    "persist and restore state correctly" in todo
  }
}

// vim: set ts=4 sw=4 et:
