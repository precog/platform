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
  def fullProjection[X](path: Path): DatasetEnum[X, SEvent, IO]
  def mask[X](path: Path): DatasetMask[X]

  //def column(path: String, selector: JPath, valueType: EType): DatasetEnum[X, SEvent, IO]
  //def columnRange(interval: Interval[ByteBuffer])(path: String, selector: JPath, valueType: EType): DatasetEnum[X, (Seq[Long], ByteBuffer), IO]
}

trait OperationsAPI {
  def ops: DatasetEnumOps
  def query: StorageEngineQueryAPI
}
