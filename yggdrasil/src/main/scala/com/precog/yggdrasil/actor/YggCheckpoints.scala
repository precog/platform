package com.precog.yggdrasil
package actor 

import com.precog.common.util.{YggCheckpoint, VectorClock, SystemCoordination}

import com.weiglewilczek.slf4s.Logging

import scalaz.{Validation, Success, Failure}

abstract class YggCheckpoints(initialCheckpoint: YggCheckpoint = YggCheckpoint(0L, VectorClock.empty)) {
  private var lastCheckpoint = initialCheckpoint
  private var pendingCheckpoints = Vector[YggCheckpoint]()

  def messagesConsumed(checkpoint: YggCheckpoint): Unit = synchronized {
    if(!pendingCheckpoints.contains(checkpoint)) {
      pendingCheckpoints = pendingCheckpoints :+ checkpoint 
    }
  }

  def persistUpTo(lowerBound: VectorClock): Unit = synchronized {
    val (before, after) = pendingCheckpoints.partition { 
      _.messageClock.isDominatedBy(lowerBound) 
    }

    if (before.nonEmpty) {
      val nextCheckpoint = before.last
      
      saveRecoveryPoint(nextCheckpoint)
      
      lastCheckpoint = nextCheckpoint
      pendingCheckpoints = after
    }
  }
  
  def latestCheckpoint = lastCheckpoint

  def pendingCheckpointCount = pendingCheckpoints.size

  def pendingCheckpointValue = pendingCheckpoints

  protected def saveRecoveryPoint(checkpoint: YggCheckpoint): Unit
}


object SystemCoordinationYggCheckpoints {
  import blueeyes.json.xschema.Extractor.Error
  def validated(shard: String, coordination: SystemCoordination): Validation[Error, YggCheckpoints] = {
    for (checkpoint <- coordination.loadYggCheckpoint(shard)) yield { 
      new SystemCoordinationYggCheckpoints(shard, coordination, checkpoint) 
    }
  }
}


class SystemCoordinationYggCheckpoints(shard: String, coordination: SystemCoordination, checkpoint: YggCheckpoint) 
extends YggCheckpoints(checkpoint) with Logging {
  protected def saveRecoveryPoint(checkpoint: YggCheckpoint) {
    coordination.saveYggCheckpoint(shard, checkpoint)    
  } 
}
