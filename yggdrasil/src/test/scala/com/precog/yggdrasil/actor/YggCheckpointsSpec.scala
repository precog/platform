package com.precog.yggdrasil
package actor 

import org.specs2.mutable._

import com.precog.common.util._

import scala.collection.mutable.ListBuffer

object YggCheckpointsSpec extends Specification {

  "YggCheckpoints" should {
    "accept last checkpoint re-assignement" in {
      val test = YggCheckpoint(1L, VectorClock.empty.update(1,1))
      val chk = new YggCheckpoints(test) {
        def saveRecoveryPoint(checkpoint: YggCheckpoint) { }
      }

      chk.latestCheckpoint must_== test 
    }
    "pending checkpoint accumulate as expected" in {
      val test = YggCheckpoint(1L, VectorClock.empty.update(1,1))
      val chk = new YggCheckpoints(test) {
        def saveRecoveryPoint(checkpoint: YggCheckpoint) { }
      }

      chk.messagesConsumed(YggCheckpoint(2L, test.messageClock.update(1,2)))
      val last = YggCheckpoint(3L, test.messageClock.update(1,3))
      chk.messagesConsumed(last)

      chk.pendingCheckpointCount must_== 2 
      chk.latestCheckpoint must_== test 
    }
    "metadata persisted causes pending checkpoints to be cleared to lower bound" in {
      val test = YggCheckpoint(1L, VectorClock.empty.update(1,1))
      val results = ListBuffer[YggCheckpoint]()
      val chk = new YggCheckpoints(test) {
        def saveRecoveryPoint(checkpoint: YggCheckpoint) { results += checkpoint }
      }

      val middle = YggCheckpoint(2L, test.messageClock.update(1,2))
      chk.messagesConsumed(middle)
      val last = YggCheckpoint(3L, test.messageClock.update(1,3))
      chk.messagesConsumed(last)

      chk.metadataPersisted(test.messageClock.update(1,2))
      
      chk.latestCheckpoint must_== middle 
      chk.pendingCheckpointCount must_== 1 
      results must_== ListBuffer(middle)
    }
  }
}
