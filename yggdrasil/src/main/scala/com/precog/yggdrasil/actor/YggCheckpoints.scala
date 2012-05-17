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
