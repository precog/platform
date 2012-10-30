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
package com.precog.shard.yggdrasil

import org.specs2.mutable.Specification

import java.io.File

import akka.actor.ActorSystem
import akka.dispatch.{ Await, ExecutionContext, Future }
import org.streum.configrity.Configuration
import scalaz.{ Copointed, Failure, Monad, Success }
import scalaz.effect.IO
import scalaz.std.stream._
import scalaz.std.string._
import scalaz.syntax.copointed._
import scalaz.syntax.foldable._

import com.precog.common.Path
import com.precog.daze.{ UserError, QueryOptions }
import com.precog.muspelheim.RawJsonColumnarTableStorageModule
import com.precog.yggdrasil.{ IdSource, ProjectionDescriptor }
import com.precog.yggdrasil.actor.{ StandaloneShardSystemActorModule, StandaloneShardSystemConfig }

trait TestYggdrasilQueryExecutor extends YggdrasilQueryExecutor
    with RawJsonColumnarTableStorageModule[Future]
    with StandaloneShardSystemActorModule {

  type YggConfig = YggdrasilQueryExecutorConfig with StandaloneShardSystemConfig

  private val groupId = new java.util.concurrent.atomic.AtomicInteger
  def newGroupId = groupId.getAndIncrement

  val yggConfig = new YggdrasilQueryExecutorConfig with StandaloneShardSystemConfig {
    val config = Configuration(Map.empty[String, String])
    val maxSliceSize = 10000
    val idSource = new IdSource {
      def nextId() = groupId.getAndIncrement
    }
  }

  val actorSystem = ActorSystem("yggdrasilQueryExecutorActorSystem")
  implicit val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)

  implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(asyncContext) with Copointed[Future] {
    def copoint[A](f: Future[A]) = Await.result(f, yggConfig.maxEvalDuration)
  }

  def status() = Future(Failure("Status not supported yet"))
  def startup() = Future { true }
  def shutdown() = Future {
    actorSystem.shutdown
    true
  }

  object Projection extends ProjectionCompanion {
    def archive(d: ProjectionDescriptor) = sys error "archive"
    def close(d: Projection) = sys error "close"
    def open(d: ProjectionDescriptor) = sys error "open"
  }

  object Table extends TableCompanion
}

class YggdrasilQueryExecutorSpec extends Specification
    with TestYggdrasilQueryExecutor {

  val options = QueryOptions()

  "the executor" should {
    "trap syntax errors in queries" in {
      val path = Path("/election/tweets")
      val query = """
        syntax error @()!*(@
      """

      val result = execute("apiKey", query, path, options)

      result must beLike {
        case Failure(UserError(errorData)) => ok
      }
    }

    "trap compilation errors in queries" in todo
    "trap runtime errors in queries" in todo
    "trap timeout errors in queries" in todo


    "asdf" in {
      val path = Path("/election/tweets")
      val query = """
        //tweets
      """

      val result = execute("apiKey", query, path, options)

      result must beLike {
        case Success(streamt) => streamt.toStream.copoint.map(_.toString).suml must be equalTo "asdf"
      }
    }

    "output valid JSON with enormous crosses/cartesians" in {
      val path = Path("/election/tweets")
      val query = """
        tweets := //tweets
        tweets' := new tweets
        tweets ~ tweets'
          tweets
      """

      val result = execute("apiKey", query, path, options)

      // Shouldn't create an evaluation error since enormous crosses
      // can't yet be detected statically. Query cost estimation may
      // change this when implemented.
      result must beLike {
        case Success(streamt) => streamt.toStream.copoint.map(_.toString).suml must be equalTo "asdf"
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
