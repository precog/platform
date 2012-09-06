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
package shard
package yggdrasil 

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

import daze._

import muspelheim.ParseEvalStack

import com.precog.common._
import com.precog.common.security._

import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.jdbm3._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._

import com.precog.util.FilesystemFileOps

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.duration._
import akka.util.Duration
import akka.util.Timeout

import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.Validation._
import scalaz.effect.IO
import scalaz.syntax.monad._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.either._

import org.streum.configrity.Configuration

trait YggdrasilQueryExecutorConfig extends 
    BaseConfig with 
    DatasetConsumersConfig with 
    ProductionShardSystemConfig {
  lazy val flatMapTimeout: Duration = config[Int]("precog.evaluator.timeout.fm", 30) seconds
  lazy val projectionRetrievalTimeout: Timeout = Timeout(config[Int]("precog.evaluator.timeout.projection", 30) seconds)
  lazy val maxEvalDuration: Duration = config[Int]("precog.evaluator.timeout.eval", 90) seconds
}

trait YggdrasilQueryExecutorComponent {
  import blueeyes.json.xschema.Extractor

  private def wrapConfig(wrappedConfig: Configuration) = {
    new YggdrasilQueryExecutorConfig {
      val config = wrappedConfig 
      val sortWorkDir = scratchDir
      val memoizationBufferSize = sortBufferSize
      val memoizationWorkDir = scratchDir

      val clock = blueeyes.util.Clock.System

      object valueSerialization extends SortSerialization[SValue] with SValueRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
      object eventSerialization extends SortSerialization[SEvent] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
      object groupSerialization extends SortSerialization[(SValue, Identities, SValue)] with GroupRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization
      object memoSerialization extends IncrementalSerialization[(Identities, SValue)] with SEventRunlengthFormatting with BinarySValueFormatting with ZippedStreamSerialization

      //TODO: Get a producer ID
      val idSource = new IdSource {
        private val source = new java.util.concurrent.atomic.AtomicLong
        def nextId() = source.getAndIncrement
      }
    }
  }
    
  def queryExecutorFactory(config: Configuration, extAccessControl: AccessControl[Future]): QueryExecutor = {
    val yConfig = wrapConfig(config)
    
    new YggdrasilQueryExecutor with BlockStoreColumnarTableModule[Future] with JDBMProjectionModule with ProductionShardSystemActorModule {
      implicit lazy val actorSystem = ActorSystem("yggdrasilExecutorActorSystem")
      implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
      val yggConfig = yConfig
      
      implicit val M: Monad[Future] with Copointed[Future] = new blueeyes.bkka.FutureMonad(asyncContext) with Copointed[Future] {
        def copoint[A](f: Future[A]) = Await.result(f, yggConfig.maxEvalDuration)
      }

      class Storage extends SystemActorStorageLike(FileMetadataStorage.load(yggConfig.dataDir, yggConfig.archiveDir, FilesystemFileOps).unsafePerformIO) {
        val accessControl = extAccessControl
      }

      val storage = new Storage

      object Projection extends JDBMProjectionCompanion {
        val fileOps = FilesystemFileOps
        def baseDir(descriptor: ProjectionDescriptor) = sys.error("todo")
        def archiveDir(descriptor: ProjectionDescriptor) = sys.error("todo")
      }

      trait TableCompanion extends BlockStoreColumnarTableCompanion {
        import scalaz.std.anyVal._
        implicit val geq: scalaz.Equal[Int] = scalaz.Equal[Int]
      }

      object Table extends TableCompanion
    }
  }
}

trait YggdrasilQueryExecutor 
    extends QueryExecutor
    with ParseEvalStack[Future]
    with IdSourceScannerModule[Future]
    with SystemActorStorageModule
    with MemoryDatasetConsumer[Future]
    with Logging  { self =>

  type YggConfig = YggdrasilQueryExecutorConfig

  def startup() = storage.start.onComplete {
    case Left(error) => logger.error("Startup of actor ecosystem failed!", error)
    case Right(_) => logger.info("Actor ecosystem started.")
  }

  def shutdown() = storage.stop.onComplete {
    case Left(error) => logger.error("An error was encountered in actor ecosystem shutdown!", error)
    case Right(_) => logger.info("Actor ecossytem shutdown complete.")
  }

  case class StackException(error: StackError) extends Exception(error.toString)

  def execute(userUID: String, query: String, prefix: Path): Validation[EvaluationError, JArray] = {
    logger.debug("Executing for %s: %s, prefix: %s".format(userUID, query,prefix))

    import EvaluationError._
    val solution: Validation[Throwable, Validation[EvaluationError, JArray]] = Validation.fromTryCatch {
      asBytecode(query) flatMap { bytecode =>
        ((systemError _) <-: (StackException(_)) <-: decorate(bytecode).disjunction.validation) flatMap { dag =>
          (systemError _) <-: evaluateDag(userUID, dag, prefix)
        }
      }
    } 

    ((systemError _) <-: solution).flatMap(identity[Validation[EvaluationError, JArray]])
  }

  def browse(userUID: String, path: Path): Future[Validation[String, JArray]] = {
    storage.userMetadataView(userUID).findChildren(path) map {
      case paths => success(JArray(paths.map( p => JString(p.toString))(collection.breakOut)))
    }
  }

  def structure(userUID: String, path: Path): Future[Validation[String, JObject]] = {
    val futRoot = storage.userMetadataView(userUID).findPathMetadata(path, JPath(""))

    def transform(children: Set[PathMetadata]): JObject = {
      // Rewrite with collect or fold?
      val (primitives, compounds) = children.partition {
        case PathValue(_, _, _) => true
        case _                  => false
      }

      val fields = compounds.map {
        case PathIndex(i, children) =>
          val path = "[%d]".format(i)
          JField(path, transform(children))
        case PathField(f, children) =>
          val path = "." + f
          JField(path, transform(children))
        case _ => throw new MatchError("Non-compound in compounds")
      }.toList

      val types = JArray(primitives.map { 
        case PathValue(t, _, _) => JString(CType.nameOf(t))
        case _ => throw new MatchError("Non-primitive in primitives")
      }.toList)

      JObject(fields :+ JField("types", types))
    }

    futRoot.map { pr => Success(transform(pr.children)) } 
  }

  def status(): Future[Validation[String, JValue]] = {
    //storage.actorsStatus.map { success(_) } 
    Future(Failure("Status not supported yet"))
  }

  private def evaluateDag(userUID: String, dag: DepGraph,prefix: Path): Validation[Throwable, JArray] = {
    withContext { ctx =>
      logger.debug("Evaluating DAG for " + userUID)
      val result = consumeEval(userUID, dag, ctx,prefix) map { events => logger.debug("Events = " + events); JArray(events.map(_._2.toJValue)(collection.breakOut)) }
      sys.error("todo: uncomment the next line (and make it work)")
      //ctx.release.release.unsafePerformIO
      logger.debug("DAG evaluated to " + result)
      result
    }
  }

  private def asBytecode(query: String): Validation[EvaluationError, Vector[Instruction]] = {
    try {
      val tree = compile(query)
      if (tree.errors.isEmpty) success(emit(tree)) 
      else failure(
        UserError(
          JArray(
            (tree.errors: Set[Error]) map {
              case Error(loc, tp) =>
                JObject(
                  JField("message", JString("Errors occurred compiling your query.")) 
                  :: JField("line", JString(loc.line))
                  :: JField("lineNum", JNum(loc.lineNum))
                  :: JField("colNum", JNum(loc.colNum))
                  :: JField("detail", JString(tp.toString))
                  :: Nil
                )
            } toList
          )
        )
      )
    } catch {
      case ex: ParseException => failure(
        UserError(
          JArray(
            JObject(
              JField("message", JString("An error occurred parsing your query."))
              :: JField("line", JString(ex.failures.head.tail.line))
              :: JField("lineNum", JNum(ex.failures.head.tail.lineNum))
              :: JField("colNum", JNum(ex.failures.head.tail.colNum))
              :: JField("detail", JString(ex.mkString))
              :: Nil
            ) :: Nil
          )
        )
      )
    }
  }
}

