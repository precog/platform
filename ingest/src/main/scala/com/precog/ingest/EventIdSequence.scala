package com.precog
package ingest

import common._
import common.util._

import akka.dispatch.Future
import akka.dispatch.MessageDispatcher

import java.util.concurrent.atomic.AtomicInteger

import scalaz.{Success, Failure}

trait EventIdSequence {
  def next(offset: Long): (Int, Int)
  def saveState(offset: Long): Unit
  def getLastOffset(): Long
  def close(): Future[Unit]
}

class SystemEventIdSequence(agent: String, coordination: SystemCoordination, blockSize: Int = 100000)(implicit dispatcher: MessageDispatcher) extends EventIdSequence {

  private case class InternalState(eventRelayState: EventRelayState) {
    private val nextSequenceId = new AtomicInteger(eventRelayState.nextSequenceId)

    val block = eventRelayState.idSequenceBlock
    val lastOffset = eventRelayState.offset

    def current = nextSequenceId.get
    def isEmpty = current > block.lastSequenceId
    def next() = if(isEmpty) sys.error("Next on empty sequence is invalid.") else
                             (block.producerId, nextSequenceId.getAndIncrement)
  }

  // How to approach this from a lazy viewpoint (deferred at this time but need to return) -nm
  // The correct way to do this is to pass the initial state as a constructor argument, and make
  // the constructor private and only expose it via the companion object's apply method. Since registering  // the relay agent (as is done in loadInitialState) is an operation that can fail, you should not be
  // able to even construct such an id sequence if you can't obtain the initial state.
  private var state: InternalState = loadInitialState

  private def loadInitialState() = {
    val eventRelayState = coordination.registerRelayAgent(agent, blockSize).getOrElse(sys.error("Unable to retrieve relay agent state."))    
    InternalState(eventRelayState)
  }

  def next(offset: Long) = {
    if(state.isEmpty) {
      state = refill(offset)
    }
    state.next
  }

  def currentRelayState(offset: Long) = {
    EventRelayState(offset, state.current, state.block)
  }

  private def refill(offset: Long): InternalState = {
    coordination.renewEventRelayState(agent, currentRelayState(offset), blockSize) match {
      case Success(ers @ EventRelayState(_,_,_)) => InternalState(ers)
      case Failure(e)                            => sys.error("Error trying to renew relay agent: " + e)
    }
  }

  def saveState(offset: Long) {
    state = coordination.saveEventRelayState(agent, currentRelayState(offset)) match {
      case Success(ers @ EventRelayState(_,_,_)) => InternalState(ers)
      case Failure(e)                            => sys.error("Error trying to save relay agent state: " + e)
    }
  }

  def close() = Future {
    coordination.close()
  }

  def getLastOffset(): Long = {
    state.lastOffset
  }

}
