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
package com.precog.shard
package util

import com.precog.util._
import com.precog.common._
import com.precog.common.security._

import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._

import blueeyes.json._

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.Timeout
import akka.util.Duration

import org.streum.configrity.Configuration

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import scalaz.effect.IO

object ShardTestInit extends App with JDBMProjectionModule with SystemActorStorageModule with StandaloneShardSystemActorModule {

  val dir = new File("./data") 
  dir.mkdirs

  class YggConfig(val config: Configuration) extends BaseConfig with StandaloneShardSystemConfig with JDBMProjectionModuleConfig {
    val maxSliceSize = config[Int]("precog.jdbm.maxSliceSize", 50000)
  }

  val yggConfig = new YggConfig(Configuration.parse("precog.storage.root = " + dir.getName))

  val metadataStorage = FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO

  val actorSystem = ActorSystem("shard-test-init")
  implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

  class Storage extends SystemActorStorageLike(metadataStorage) {
    val accessControl = new UnrestrictedAccessControl[Future]()
  }

  val storage = new Storage

  object Projection extends JDBMProjectionCompanion {
    val fileOps = FilesystemFileOps
    def baseDir(descriptor: ProjectionDescriptor) = sys.error("todo")
    def archiveDir(descriptor: ProjectionDescriptor) = sys.error("todo")
  }

  def usage() {
    println("usage: command [{datapath}={filename}]")
    System.exit(1)
  }

  private val seqId = new AtomicInteger(0) 

  def run(loads: Array[String]) {
    Await.result(storage.start(), Duration(30, "seconds"))
    val timeout = Timeout(30000) 
    loads.foreach{ insert(_, timeout) }
    Await.result(storage.stop(), Duration(30, "seconds"))
  }

  def insert(load: String, timeout: Timeout) {
    val parts = load.split("=")

    val filename = parts(1)
    val path = parts(0)

    IOUtils.readFileToString(new File(filename)).map { data =>
      val json = JParser.parse(data)

      val emptyMetadata: Map[JPath, Set[UserMetadata]] = Map.empty

      json match {
        case JArray(elements) => 
          val fut = storage.storeBatch(elements.map{ value =>
            println(value.renderCompact)
            EventMessage(EventId(0, seqId.getAndIncrement), Event("root", Path(path), Some("root"), value, emptyMetadata))
          })
          //Await.result(fut, Duration(30, "seconds"))
          Await.result(fut, timeout.duration) // FIXME: is correct, or the line above?
        case single           =>
          val fut = storage.store(EventMessage(EventId(0, seqId.getAndIncrement), Event("root", Path(path), Some("root"), single, emptyMetadata)))
          //Await.result(fut, Duration(30, "seconds"))
          Await.result(fut, timeout.duration) // FIXME: is correct, or the line above?
      }
    } except {
      err => println(err); IO(PrecogUnit)
    } unsafePerformIO
  }

  if(args.length == 0) usage else run(args)

}
