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
package daze

import com.precog.yggdrasil._
import com.precog.yggdrasil.kafka._
import com.precog.yggdrasil.leveldb._
import com.precog.yggdrasil.shard._
import com.precog.yggdrasil.util.Enumerators
import com.precog.analytics.Path
import StorageMetadata._

import akka.dispatch.Future
import akka.util.duration._
import blueeyes.json.JPath
import java.io.File
import scalaz.{Identity => _, _}
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.set._
import scalaz.std.AllInstances._

import Iteratee._

trait StorageEngineInsertAPI 

trait StorageEngineQueryAPI {
  def fullProjection[X](path: Path): Future[DatasetEnum[X, SEvent, IO]]
  def mask[X](path: Path): DatasetMask[X]

  //def column(path: String, selector: JPath, valueType: EType): DatasetEnum[X, SEvent, IO]
  //def columnRange(interval: Interval[ByteBuffer])(path: String, selector: JPath, valueType: EType): DatasetEnum[X, (Seq[Long], ByteBuffer), IO]
}

trait OperationsAPI {
  def query: StorageEngineQueryAPI
  def ops: DatasetEnumOps
}

