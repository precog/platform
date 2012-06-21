package com.precog
package shard
package yggdrasil 

import blueeyes.json.JPath
import blueeyes.json.JsonAST._

import daze._
import daze.memoization._

import muspelheim.ParseEvalStack

import com.precog.common._
import com.precog.common.security._

import com.precog.yggdrasil._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.serialization._

import com.precog.util.FilesystemFileOps

import akka.actor.ActorSystem
import akka.dispatch._
import akka.util.duration._
import akka.util.Duration
import akka.util.Timeout

import com.weiglewilczek.slf4s.Logging

import scalaz.{Success, Failure, Validation}
import scalaz.Validation._
import scalaz.effect.IO
import scalaz.syntax.monad._

import org.streum.configrity.Configuration

trait YggdrasilQueryExecutorConfig extends 
    BaseConfig with 
    YggEnumOpsConfig with 
    LevelDBQueryConfig with 
    DiskMemoizationConfig with 
    DatasetConsumersConfig with 
    IterableDatasetOpsConfig with 
    ProductionActorConfig {
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
    
  def queryExecutorFactory(config: Configuration, extAccessControl: AccessControl): QueryExecutor = {
    val yConfig = wrapConfig(config)
    
    new YggdrasilQueryExecutor {
      //trait Storage extends ProductionActorEcosystem[IterableDataset] with ActorYggShard[IterableDataset] with LevelDBProjectionsActorModule

      lazy val actorSystem = ActorSystem("yggdrasil_exeuctor_actor_system")
      implicit lazy val asyncContext = ExecutionContext.defaultExecutionContext(actorSystem)
      val yggConfig = yConfig
      
      object ops extends Ops 
      object query extends QueryAPI 

      // Early initializers FTW (and to avoid cake badness with metadataStorage)
      class Storage extends {
        type YggConfig = YggdrasilQueryExecutorConfig
        val yggConfig = yConfig
        val metadataStorage = FileMetadataStorage.load(yggConfig.dataDir, new FilesystemFileOps {}).unsafePerformIO
        val accessControl = extAccessControl
      } with ProductionActorEcosystem[IterableDataset] with ActorYggShard[IterableDataset] with LevelDBProjectionsActorModule
      val storage = new Storage
    }
  }
}

trait YggdrasilQueryExecutor 
    extends QueryExecutor
    with ParseEvalStack
    with IterableDatasetOpsComponent
    with LevelDBQueryComponent 
    with MemoryDatasetConsumer
    with DiskIterableMemoizationComponent
    with Logging  { self =>
  override type Dataset[E] = IterableDataset[E]
  override type Memoable[E] = Iterable[E]

  type YggConfig = YggdrasilQueryExecutorConfig
  type Storage <: ActorYggShard[IterableDataset]

  def startup() = storage.actorsStart.onComplete {
    case Left(error) => logger.error("Startup of actor ecosystem failed!", error)
    case Right(_) => logger.info("Actor ecosystem started.")
  }

  def shutdown() = storage.actorsStop.onComplete {
    case Left(error) => logger.error("An error was encountered in actor ecosystem shutdown!", error)
    case Right(_) => logger.info("Actor ecossytem shutdown complete.")
  }

  case class StackException(error: StackError) extends Exception(error.toString)

  def execute(userUID: String, query: String): Validation[EvaluationError, JArray] = {
    logger.debug("Executing for %s: %s".format(userUID, query))

    import EvaluationError._
    implicit val M = Validation.validationMonad[EvaluationError]
    
    val solution: Validation[Throwable, Validation[EvaluationError, JArray]] = Validation.fromTryCatch {
      asBytecode(query) flatMap { bytecode =>
        Validation.fromEither(decorate(bytecode)).bimap(
          error => systemError(StackException(error)),
          dag   => evaluateDag(userUID, dag).fail.map(systemError(_)).validation
        ).join
      }
    } 

    solution.fail.map(systemError(_)).validation.join
  }

  def browse(userUID: String, path: Path): Future[Validation[String, JArray]] = {
    storage.userMetadataView(userUID).findChildren(path) map {
      case paths => success(JArray(paths.map( p => JString(p.toString))(collection.breakOut)))
    }
  }

  def structure(userUID: String, path: Path): Future[Validation[String, JObject]] = {
    val futRoot = storage.userMetadataView(userUID).findPathMetadata(path, JPath(""))

    def transform(children: Set[PathMetadata]): JObject = {
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
      }.toList

      val types = JArray(primitives.map { 
        case PathValue(t, _, _) => JString(CType.nameOf(t))
      }.toList)

      JObject(fields :+ JField("types", types))
    }

    futRoot.map { pr => Success(transform(pr.children)) } 
  }

  def status(): Future[Validation[String, JValue]] = {
    //storage.actorsStatus.map { success(_) } 
    Future(Failure("Status not supported yet"))
  }

  private def evaluateDag(userUID: String, dag: DepGraph): Validation[Throwable, JArray] = {
    withContext { ctx =>
      logger.debug("Evaluating DAG for " + userUID)
      val result = consumeEval(userUID, dag, ctx) map { events => logger.debug("Events = " + events); JArray(events.map(_._2.toJValue)(collection.breakOut)) }
      ctx.release.release.unsafePerformIO
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
                  :: JField("lineNum", JInt(loc.lineNum))
                  :: JField("colNum", JInt(loc.colNum))
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
              :: JField("lineNum", JInt(ex.failures.head.tail.lineNum))
              :: JField("colNum", JInt(ex.failures.head.tail.colNum))
              :: JField("detail", JString(ex.mkString))
              :: Nil
            ) :: Nil
          )
        )
      )
    }
  }
}

