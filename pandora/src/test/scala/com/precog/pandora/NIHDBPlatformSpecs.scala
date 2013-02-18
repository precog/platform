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
package com.precog.pandora

import com.precog.common.Path
import com.precog.common.VectorCase
import com.precog.common.accounts._
import com.precog.common.kafka._
import com.precog.common.security._

import com.precog.bytecode.JType

import com.precog.daze._

import com.precog.quirrel._
import com.precog.quirrel.emitter._
import com.precog.quirrel.parser._
import com.precog.quirrel.typer._

import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.nihdb._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._
import com.precog.yggdrasil.test.YId

import com.precog.muspelheim._
import com.precog.niflheim._
import com.precog.util.FilesystemFileOps

import akka.actor.ActorSystem
import akka.actor.Props
import akka.dispatch._
import akka.pattern.gracefulStop
import akka.util.{Duration, Timeout}
import akka.util.duration._

import java.io.File

import blueeyes.bkka._
import blueeyes.json._

import org.slf4j.LoggerFactory

import org.specs2.mutable._
import org.specs2.specification.Fragments
  
import scalaz._
import scalaz.std.anyVal._
import scalaz.syntax.monad._
import scalaz.syntax.copointed._
import scalaz.effect.IO

import org.streum.configrity.Configuration
import org.streum.configrity.io.BlockFormat

trait NIHDBPlatformSpecs extends ParseEvalStackSpecs[Future] 
    with LongIdMemoryDatasetConsumer[Future]
    with NIHDBColumnarTableModule 
    with NIHDBStorageMetadataSource { self =>
      
  override def map(fs: => Fragments): Fragments = step { startup() } ^ fs ^ step { shutdown() }
      
  lazy val psLogger = LoggerFactory.getLogger("com.precog.pandora.PlatformSpecs")

  abstract class YggConfig extends ParseEvalStackSpecConfig
      with IdSourceConfig
      with EvaluatorConfig
      with StandaloneShardSystemConfig
      with ColumnarTableModuleConfig
      with BlockStoreColumnarTableModuleConfig {
    val cookThreshold = 10
    val ingestConfig = None
  }

  object yggConfig extends YggConfig

  implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(asyncContext) with Copointed[Future] {
    def copoint[A](f: Future[A]) = Await.result(f, yggConfig.maxEvalDuration)
  }

  val accountFinder = None

  def Evaluator[N[+_]](N0: Monad[N])(implicit mn: Future ~> N, nm: N ~> Future) = 
    new Evaluator[N](N0)(mn,nm) with IdSourceScannerModule {
      val report = new LoggingQueryLogger[N, instructions.Line] with ExceptionQueryLogger[N, instructions.Line] {
        val M = N0
      }
      class YggConfig extends EvaluatorConfig {
        val idSource = new FreshAtomicIdSource
        val maxSliceSize = 10
      }
      val yggConfig = new YggConfig
  }

  override val accessControl = new UnrestrictedAccessControl[Future]

  val storageTimeout = Timeout(300 * 1000)

  val masterChef = actorSystem.actorOf(Props(Chef(VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)), VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))))
  //println("Datadir = " + yggConfig.dataDir)
  //println("ArchiveDir = " + yggConfig.archiveDir)

  val projectionsActor = actorSystem.actorOf(Props(new NIHDBProjectionsActor(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps, masterChef, yggConfig.cookThreshold, storageTimeout, accessControl)))

  val report = new LoggingQueryLogger[Future, instructions.Line] with ExceptionQueryLogger[Future, instructions.Line] {
    implicit def M = self.M
  }

  trait TableCompanion extends NIHDBColumnarTableCompanion

  object Table extends TableCompanion

  def startup() { }
  
  def shutdown() {
    Await.result(gracefulStop(projectionsActor, Duration(5, "minutes")), Duration(3, "minutes"))
    actorSystem.shutdown()
  }
}

class NIHDBBasicValidationSpecs extends BasicValidationSpecs with NIHDBPlatformSpecs

class NIHDBHelloQuirrelSpecs extends HelloQuirrelSpecs with NIHDBPlatformSpecs

class NIHDBLogisticRegressionSpecs extends LogisticRegressionSpecs with NIHDBPlatformSpecs

class NIHDBLinearRegressionSpecs extends LinearRegressionSpecs with NIHDBPlatformSpecs

class NIHDBClusteringSpecs extends ClusteringSpecs with NIHDBPlatformSpecs

class NIHDBMiscStackSpecs extends MiscStackSpecs with NIHDBPlatformSpecs

class NIHDBNonObjectStackSpecs extends NonObjectStackSpecs with NIHDBPlatformSpecs

class NIHDBRankSpecs extends RankSpecs with NIHDBPlatformSpecs

class NIHDBRenderStackSpecs extends RenderStackSpecs with NIHDBPlatformSpecs

class NIHDBUndefinedLiteralSpecs extends UndefinedLiteralSpecs with NIHDBPlatformSpecs
