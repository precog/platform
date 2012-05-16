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
    if(!pendingCheckpoints.contains(checkpoint)) {
      pendingCheckpoints = pendingCheckpoints :+ checkpoint 
    }
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


class SystemCoordinationYggCheckpoints(shard: String, coordination: SystemCoordination) extends YggCheckpoints with Logging {
  
  lastCheckpoint = coordination.loadYggCheckpoint(shard) match {
    case Success(checkpoint) => checkpoint
    case Failure(e)          => sys.error("Error loading shard checkpoint from zookeeper")
  }
  
  override def messagesConsumed(checkpoint: YggCheckpoint) {
    logger.info("Recording new consumption checkpoint: " + checkpoint)
    super.messagesConsumed(checkpoint)
  }

  override def metadataPersisted(messageClock: VectorClock) {
    logger.info("Recording new metadata checkpoint: " + messageClock)
    super.metadataPersisted(messageClock)
  }

  protected def saveRecoveryPoint(checkpoint: YggCheckpoint) {
    coordination.saveYggCheckpoint(shard, checkpoint)    
  } 
}

// vim: set ts=4 sw=4 et:
