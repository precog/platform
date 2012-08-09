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
package ragnarok

import yggdrasil.ProjectionDescriptor
import yggdrasil.jdbm3._
import yggdrasil.actor._
import yggdrasil.table.BlockStoreColumnarTableModule
import yggdrasil.metadata.FileMetadataStorage

import common.security.UnlimitedAccessControl

import util.{ FileOps, FilesystemFileOps }

import akka.actor.ActorSystem
import akka.dispatch.{ Future, Await }

import org.streum.configrity.Configuration

import scalaz._


trait StandalonePerfTestRunner[T] extends EvaluatingPerfTestRunner[Future, T]
  with SystemActorStorageModule
  with StandaloneShardSystemActorModule {

  // implicit def M: Monad[Future]

  type YggConfig <: StandalonePerfTestRunnerConfig 
  
  trait StandalonePerfTestRunnerConfig extends EvaluatingPerfTestRunnerConfig with StandaloneShardSystemConfig

  class Storage extends SystemActorStorageLike(FileMetadataStorage.load(yggConfig.dataDir, FilesystemFileOps).unsafePerformIO) {
    val accessControl = new UnlimitedAccessControl[Future]()
  }

  val storage = new Storage

  def startup() {
    Await.result(storage.start(), yggConfig.maxEvalDuration)
  }

  def shutdown() {
    Await.result(storage.stop(), yggConfig.maxEvalDuration)
  }
}


final class JDBMPerfTestRunner[T](val timer: Timer[T], val userUID: String, val optimize: Boolean,
      val actorSystem: ActorSystem)(implicit val M: Monad[Future])
    extends StandalonePerfTestRunner[T]
    with BlockStoreColumnarTableModule[Future]
    with JDBMProjectionModule { self =>

  type YggConfig = StandalonePerfTestRunnerConfig 
  object yggConfig extends YggConfig {
    val userUID = self.userUID
    val optimize = self.optimize
    override val config = Configuration parse {
      Option(System.getProperty("precog.storage.root")) map { "precog.storage.root = " + _ } getOrElse { "" }
    }
  }

  object Projection extends JDBMProjectionCompanion {
    val fileOps = FilesystemFileOps
    def baseDir(descriptor: ProjectionDescriptor) = sys.error("todo")
  }
}
