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
package com.precog.ragnarok

import com.precog.common.accounts._
import com.precog.common.security._
import com.precog.daze._
import com.precog.niflheim._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.nihdb._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._
import com.precog.util.{ FileOps, FilesystemFileOps, XLightWebHttpClientModule }

import java.io.File

import akka.actor.{ActorSystem, Props}
import akka.routing.RoundRobinRouter
import akka.dispatch.{ Future, Await, ExecutionContext }
import akka.util.{ Timeout, Duration }
import akka.pattern.gracefulStop

import org.streum.configrity.Configuration

import blueeyes.bkka._

import scalaz._
import scalaz.effect._
import scalaz.std.anyVal._

final class NIHDBPerfTestRunner[T](val timer: Timer[T], val apiKey: APIKey, val optimize: Boolean, _rootDir: Option[File], testTimeout: Duration = Duration(120, "seconds"))
    extends EvaluatingPerfTestRunner[Future, T]
    with XLightWebHttpClientModule[Future]
    with NIHDBColumnarTableModule
    with NIHDBStorageMetadataSource
    with NIHDBIngestSupport { self =>
    // with StandaloneActorProjectionSystem
    // with SliceColumnarTableModule[Future, Array[Byte]] { self =>

  trait NIHDBPerfTestRunnerConfig 
      extends BaseConfig
      with EvaluatingPerfTestRunnerConfig
      with BlockStoreColumnarTableModuleConfig
      with EvaluatorConfig

  implicit val actorSystem = ActorSystem("NIHDBPerfTestRunner")
  implicit val M = new UnsafeFutureComonad(actorSystem.dispatcher, testTimeout)

  type YggConfig = NIHDBPerfTestRunnerConfig
  object yggConfig extends NIHDBPerfTestRunnerConfig {
    val ingestConfig = None
    val apiKey = self.apiKey
    val optimize = self.optimize
    val commandLineConfig = Configuration.parse(_rootDir map ("precog.storage.root = " + _) getOrElse "")
    override val config = (Configuration parse {
      Option(System.getProperty("precog.storage.root")) map ("precog.storage.root = " + _) getOrElse "" }) ++ commandLineConfig
    val cookThreshold = 10000
  }

  yggConfig.dataDir.mkdirs()

  trait TableCompanion extends NIHDBColumnarTableCompanion
  object Table extends TableCompanion

  val accountFinder = new StaticAccountFinder[Future]("", "")
  val accessControl = new DirectAPIKeyFinder(new UnrestrictedAPIKeyManager[Future](blueeyes.util.Clock.System))
  val permissionsFinder = new PermissionsFinder(accessControl, accountFinder, new org.joda.time.Instant())

  val storageTimeout = Timeout(testTimeout)

  private def makeChef = Chef(
    VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)),
    VersionedSegmentFormat(Map(1 -> V1SegmentFormat)))

  val chefs = (1 to 4).map { _ => actorSystem.actorOf(Props(makeChef)) }
  val chef = actorSystem.actorOf(Props[Chef].withRouter(RoundRobinRouter(chefs)))
  val projectionsActor = actorSystem.actorOf(Props(
    new NIHDBProjectionsActor(
      yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps,
      chef, yggConfig.cookThreshold, storageTimeout,
      permissionsFinder)
  ))

  def Evaluator[N[+_]](N0: Monad[N])(implicit mn: Future ~> N, nm: N ~> Future): EvaluatorLike[N] = {
    new Evaluator[N](N0) {
      type YggConfig = self.YggConfig
      val yggConfig = self.yggConfig
      val report = LoggingQueryLogger[N](N0)
      def freshIdScanner = self.freshIdScanner
    }
  }

  def startup() {}

  def shutdown() {
    Await.result(gracefulStop(projectionsActor, storageTimeout.duration), storageTimeout.duration)
    actorSystem.shutdown()
  }
}
