package com.precog.shard
package yggdrasil

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

  type YggConfig = BaseYggdrasilQueryExecutorConfig with StandaloneShardSystemConfig

  private val groupId = new java.util.concurrent.atomic.AtomicInteger
  def newGroupId = groupId.getAndIncrement

  val yggConfig = new BaseYggdrasilQueryExecutorConfig with StandaloneShardSystemConfig {
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
