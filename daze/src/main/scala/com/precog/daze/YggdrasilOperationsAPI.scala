package com.precog.daze

import com.precog.yggdrasil._
import com.precog.yggdrasil.shard._
import com.precog.yggdrasil.util._

import java.io.File

import scalaz.Order
import scalaz.effect._

trait YggdrasilStorage {
  def storage: YggShard
}

trait YggdrasilOperationsAPI extends OperationsAPI { self: YggdrasilStorage =>
  def asyncContext: akka.dispatch.ExecutionContext

  object ops extends DatasetEnumOps {
    def sort[X](enum: DatasetEnum[X, SEvent, IO], memoId: Option[Int])(implicit order: Order[SEvent]): DatasetEnum[X, SEvent, IO] = {
      DatasetEnum(Enumerators.sort[X](enum.enum, storage.yggConfig.sortBufferSize, storage.yggConfig.newWorkDir, enum.descriptor))
    }
    
    def memoize[X](enum: DatasetEnum[X, SEvent, IO], memoId: Int): DatasetEnum[X, SEvent, IO] = enum      // TODO
  }

  object query extends LevelDBQueryAPI {
    def asyncContext = self.asyncContext
    def storage = self.storage
  }
}

// vim: set ts=4 sw=4 et:
