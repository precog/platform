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
package com.precog.ingest

import com.precog.common._
import com.precog.common.ingest._
import com.precog.util.PrecogUnit

import java.util.concurrent.atomic.AtomicInteger

import scalaz.{Success, Failure}

trait EventIdSequence {
  def next(offset: Long): EventId
  def saveState(offset: Long): PrecogUnit
  def getLastOffset(): Long
}

class SystemEventIdSequence private(agent: String,
                                    coordination: SystemCoordination,
                                    private var state: SystemEventIdSequence.InternalState,
                                    blockSize: Int) /*(implicit executor: ExecutionContext) */ extends EventIdSequence {
  import SystemEventIdSequence.InternalState

  def next(offset: Long) = {
    if(state.isEmpty) {
      state = refill(offset)
    }
    state.next()
  }

  def currentRelayState(offset: Long) = {
    EventRelayState(offset, state.current, state.block)
  }

  private def refill(offset: Long): InternalState = {
    coordination.renewEventRelayState(agent, offset, state.block.producerId, blockSize) match {
      case Success(ers @ EventRelayState(_,_,_)) => InternalState(ers)
      case Failure(e)                            => sys.error("Error trying to renew relay agent: " + e)
    }
  }

  def saveState(offset: Long) = {
    state = coordination.saveEventRelayState(agent, currentRelayState(offset)) match {
      case Success(ers @ EventRelayState(_,_,_)) => InternalState(ers)
      case Failure(e)                            => sys.error("Error trying to save relay agent state: " + e)
    }

    PrecogUnit
  }

  def getLastOffset(): Long = {
    state.lastOffset
  }
}

object SystemEventIdSequence {
  class UnableToRetrieveRelayAgentState() extends Exception("Unable to retrieve relay agent state.")

  private[ingest] case class InternalState(eventRelayState: EventRelayState) {
    private val nextSequenceId = new AtomicInteger(eventRelayState.nextSequenceId)

    val block = eventRelayState.idSequenceBlock
    val lastOffset = eventRelayState.offset

    def current = nextSequenceId.get
    def isEmpty = current > block.lastSequenceId
    def next() = if(isEmpty) sys.error("Next on empty sequence is invalid.") else
      EventId(block.producerId, nextSequenceId.getAndIncrement)
  }

  def apply(agent: String, coordination: SystemCoordination, blockSize: Int = 100000): SystemEventIdSequence = {
    def loadInitialState() = {
      val eventRelayState = coordination.registerRelayAgent(agent, blockSize).getOrElse(throw new UnableToRetrieveRelayAgentState)
      InternalState(eventRelayState)
    }

    new SystemEventIdSequence(agent, coordination, loadInitialState(), blockSize)
  }
}
