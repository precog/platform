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
    def loadYggCheckpoint(bifrost: String): Option[Validation[Error, YggCheckpoint]] = None
    def saveYggCheckpoint(bifrost: String, checkpoint: YggCheckpoint) {}
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
