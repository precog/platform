package com.precog.standalone

import akka.dispatch.{ExecutionContext, Future, Promise}
import akka.util.duration._
import akka.util.Duration

import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }

import com.precog.common._
import com.precog.common.jobs._
import com.precog.common.accounts._
import com.precog.common.security._
import com.precog.daze._
import com.precog.muspelheim._
import com.precog.shard._
import com.precog.yggdrasil._
import com.precog.yggdrasil.actor._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.serialization._
import com.precog.yggdrasil.table._
import com.precog.yggdrasil.util._

import com.weiglewilczek.slf4s.Logging

import java.nio.CharBuffer

import scalaz._
import scalaz.Validation._
import scalaz.effect.IO
import scalaz.syntax.monad._
import scalaz.syntax.bifunctor._
import scalaz.syntax.std.either._
import scalaz.syntax.std.option._
import scala.collection.JavaConverters._

trait StandaloneQueryExecutorConfig extends BaseConfig
      with ColumnarTableModuleConfig
      with BlockStoreColumnarTableModuleConfig
      with ShardQueryExecutorConfig
      with IdSourceConfig
      with ManagedQueryModuleConfig
      with ShardConfig {
  def maxSliceSize = config[Int]("engine.max_slice_size", 10000)
  def smallSliceSize = config[Int]("engine.small_slice_size", 8)

  val shardId = "standalone"

  val idSource = new FreshAtomicIdSource

  def masterAPIKey: String = config[String]("masterAccount.apiKey", "12345678-9101-1121-3141-516171819202")

  def maxEvalDuration: Duration = config[Int]("precog.evaluator.timeout.eval", 90) seconds
  def jobPollFrequency: Duration = config[Int]("jobs.poll_interval", 2) seconds

  val clock = blueeyes.util.Clock.System

  val ingestConfig = None
}

trait StandaloneQueryExecutor
    extends ManagedPlatform
    with ShardQueryExecutorPlatform[Future]
    with Logging { platform =>

  type YggConfig <: StandaloneQueryExecutorConfig

  def executionContext: ExecutionContext

  override def defaultTimeout = yggConfig.maxEvalDuration

  implicit val nt = NaturalTransformation.refl[Future]
  def executor = new ShardQueryExecutor[Future](M) {
    val M = platform.M
    type YggConfig = platform.YggConfig
    val yggConfig = platform.yggConfig
    val queryReport = LoggingQueryLogger[Future](M)
    def freshIdScanner = platform.freshIdScanner
  }

  protected def executor(implicit shardQueryMonad: JobQueryTFMonad): QueryExecutor[JobQueryTF, StreamT[JobQueryTF, Slice]] = {
    implicit val mn = new (Future ~> JobQueryTF) {
      def apply[A](fut: Future[A]) = fut.liftM[JobQueryT]
    }

    new ShardQueryExecutor[JobQueryTF](shardQueryMonad) {
      val M = platform.M
      type YggConfig = platform.YggConfig
      val yggConfig = platform.yggConfig
      val queryReport = errorReport[Option[FaultPosition]](shardQueryMonad, implicitly[Decomposer[Option[FaultPosition]]])
      def freshIdScanner = platform.freshIdScanner
    } map { case (faults, result) =>
      result
    }
  }

  def asyncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, JobId]]] = {
    logger.debug("Creating new async executor for %s => %s".format(apiKey, executionContext))
    Promise.successful(Success(new AsyncQueryExecutor {
      def executionContext: ExecutionContext = platform.executionContext
    }))
  }

  def syncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, (Option[JobId], StreamT[Future, Slice])]]] = {
    logger.debug("Creating new sync executor for %s => %s".format(apiKey, executionContext))
    Promise.successful(Success(new SyncQueryExecutor {
      def executionContext: ExecutionContext = platform.executionContext
    }))
  }

}
