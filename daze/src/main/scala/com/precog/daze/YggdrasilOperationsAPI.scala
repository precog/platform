package com.precog.daze

import com.precog.yggdrasil._
import com.precog.yggdrasil.shard._
import com.precog.yggdrasil.util._

import java.io.File

import scalaz.Order
import scalaz.effect._

trait YggdrasilStorage {
  def storage: StorageShard
}

trait YggdrasilOperationsAPI extends OperationsAPI { self: YggdrasilStorage =>
  def asyncContext: akka.dispatch.ExecutionContext
  def yggdrasilConfig: YggConfig

  object ops extends DatasetEnumOps {
    def sort[X](enum: DatasetEnum[X, SEvent, IO], memoId: Option[Int])(implicit order: Order[SEvent]): DatasetEnum[X, SEvent, IO] = {
      DatasetEnum(Enumerators.sort[X](enum.enum, yggdrasilConfig.sortBufferSize, yggdrasilConfig.workDir, enum.descriptor))
    }
    
    def memoize[X](enum: DatasetEnum[X, SEvent, IO], memoId: Int): DatasetEnum[X, SEvent, IO] = enum      // TODO
  }

  object query extends LevelDBQueryAPI {
    def asyncContext = self.asyncContext
    def storage = self.storage
  }
}

trait YggConfig {
  def workDir: File
  def sortBufferSize: Int
}

trait DefaultYggConfig {
  def yggdrasilConfig = new YggConfig {
    def workDir = {
      val tempFile = File.createTempFile("leveldb_tmp", "workdir")
      tempFile.delete //todo: validated
      tempFile.mkdir //todo: validated
      tempFile
    }

    def sortBufferSize = 100000
  }
}


// vim: set ts=4 sw=4 et:
