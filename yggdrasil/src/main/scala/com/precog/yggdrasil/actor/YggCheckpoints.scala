package com.precog
package yggdrasil
package actor 

import com.precog.common.util.{YggCheckpoint, VectorClock, SystemCoordination}

import com.weiglewilczek.slf4s.Logging

import scalaz.{Success, Failure}

trait YggCheckpoints {

  protected var lastCheckpoint = YggCheckpoint(0L, VectorClock.empty)
  private var pendingCheckpoints = Vector[YggCheckpoint]()

  def messagesConsumed(checkpoint: YggCheckpoint) {
    pendingCheckpoints = pendingCheckpoints :+ checkpoint 
  }

  def metadataPersisted(messageClock: VectorClock) {
    val (before, after) = pendingCheckpoints.span { 
      _.messageClock.isLowerBoundOf(messageClock) 
    }

    if(before.size > 0) {
      val nextCheckpoint = before.last
      
      saveRecoveryPoint(nextCheckpoint)
      
      lastCheckpoint = nextCheckpoint
      pendingCheckpoints = after
    }
  }
  
  def latestCheckpoint() = lastCheckpoint

  def pendingCheckpointCount() = pendingCheckpoints.size

  def pendingCheckpointValue() = pendingCheckpoints

  protected def saveRecoveryPoint(checkpoint: YggCheckpoint): Unit
}

class TestYggCheckpoints extends YggCheckpoints with Logging {
  protected def saveRecoveryPoint(checkpoint: YggCheckpoint) {
    logger.info("[PLACEHOLDER - TODO] saving shard recovery point to zookeeper. " + checkpoint)
  } 
}

class SystemCoordinationYggCheckpoints(shard: String, coordination: SystemCoordination) extends YggCheckpoints with Logging {
  
  lastCheckpoint = coordination.loadYggCheckpoint(shard) match {
    case Success(checkpoint) => checkpoint
    case Failure(e)          => sys.error("Error loading shard checkpoint from zookeeper")
  }
  
  override def messagesConsumed(checkpoint: YggCheckpoint) {
    logger.debug("Recording new consumption checkpoint: " + checkpoint)
    super.messagesConsumed(checkpoint)
  }

  override def metadataPersisted(messageClock: VectorClock) {
    logger.debug("Recording new metadata checkpoint: " + messageClock)
    super.metadataPersisted(messageClock)
  }

  protected def saveRecoveryPoint(checkpoint: YggCheckpoint) {
    coordination.saveYggCheckpoint(shard, checkpoint)    
  } 
}

// vim: set ts=4 sw=4 et:
