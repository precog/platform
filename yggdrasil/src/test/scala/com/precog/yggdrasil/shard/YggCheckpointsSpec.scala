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
package shard

import org.specs2.mutable._

import com.precog.common.util._

import scala.collection.mutable.ListBuffer

object YggCheckpointsSpec extends Specification {

  "YggCheckpoints" should {
    "accept last checkpoint re-assignement" in {
      val test = YggCheckpoint(1L, VectorClock.empty.update(1,1))
      val chk = new YggCheckpoints {
        lastCheckpoint = test 

        def saveRecoveryPoint(checkpoint: YggCheckpoint) { }
      }

      chk.latestCheckpoint must_== test 
    }
    "pending checkpoint accumulate as expected" in {
      val test = YggCheckpoint(1L, VectorClock.empty.update(1,1))
      val chk = new YggCheckpoints {
        lastCheckpoint = test 

        def saveRecoveryPoint(checkpoint: YggCheckpoint) { }
      }

      chk.messagesConsumed(YggCheckpoint(2L, test.messageClock.update(1,2)))
      val last = YggCheckpoint(3L, test.messageClock.update(1,3))
      chk.messagesConsumed(last)

      chk.pendingCheckpointCount() must_== 2 
      chk.latestCheckpoint must_== test 
    }
    "metadata persisted causes pending checkpoints to be cleared to lower bound" in {
      val test = YggCheckpoint(1L, VectorClock.empty.update(1,1))
      val results = ListBuffer[YggCheckpoint]()
      val chk = new YggCheckpoints {
        lastCheckpoint = test 

        def saveRecoveryPoint(checkpoint: YggCheckpoint) { results += checkpoint }
      }

      val middle = YggCheckpoint(2L, test.messageClock.update(1,2))
      chk.messagesConsumed(middle)
      val last = YggCheckpoint(3L, test.messageClock.update(1,3))
      chk.messagesConsumed(last)

      chk.metadataPersisted(test.messageClock.update(1,2))
      
      chk.latestCheckpoint must_== middle 
      chk.pendingCheckpointCount() must_== 1 
      results must_== ListBuffer(middle)

    }
  }
}
