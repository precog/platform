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
