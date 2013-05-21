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

import com.precog.daze._

import com.precog.util._
import com.precog.common._
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.muspelheim._

import akka.dispatch.{ Future, ExecutionContext }

import org.joda.time.DateTime

import java.nio.charset._
import java.nio.channels.WritableByteChannel
import java.nio.{ CharBuffer, ByteBuffer, ReadOnlyBufferException }
import java.io.{ File, FileOutputStream }

import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }
import blueeyes.core.http.MimeTypes

import scalaz._

/**
 * A `ManagedPlatform` extends `Platform` by allowing the
 * creation of a `BasicQueryExecutor` that can also allows "synchronous" (but
 * managed) queries and asynchronous queries.
 */
trait ManagedPlatform extends Platform[Future, StreamT[Future, CharBuffer]] with ManagedQueryModule { self =>

  /**
   * Returns an `Platform` whose execution returns a `JobId` rather
   * than a `StreamT[Future, CharBuffer]`.
   */
  def asynchronous: AsyncPlatform[Future] = {
    new AsyncPlatform[Future] {
      def executorFor(apiKey: APIKey) = self.asyncExecutorFor(apiKey)
      def metadataClient = self.metadataClient
    }
  }

  /**
   * Returns a `Platform` whose execution returns both the
   * streaming results and its `JobId`. Note that the reults will not be saved
   * to job.
   */
  def synchronous: SyncPlatform[Future] = {
    new SyncPlatform[Future] {
      def executorFor(apiKey: APIKey) = self.syncExecutorFor(apiKey)
      def metadataClient = self.metadataClient
    }
  }

  def errorReport[A](implicit shardQueryMonad: ShardQueryMonad, decomposer0: Decomposer[A]): QueryLogger[ShardQuery, A] = {
    import scalaz.syntax.monad._

    implicit val M = shardQueryMonad.M

    shardQueryMonad.jobId map { jobId0 =>
      val lift = new (Future ~> ShardQuery) {
        def apply[A](fa: Future[A]) = fa.liftM[JobQueryT]
      }

      new JobQueryLogger[ShardQuery, A] with ShardQueryLogger[ShardQuery, A] with TimingQueryLogger[ShardQuery, A] {
        val M = shardQueryMonad
        val jobManager = self.jobManager.withM[ShardQuery](lift, implicitly, shardQueryMonad.M, shardQueryMonad)
        val jobId = jobId0
        val clock = yggConfig.clock
        val decomposer = decomposer0
      }
    } getOrElse {
      new LoggingQueryLogger[ShardQuery, A] with ShardQueryLogger[ShardQuery, A] with TimingQueryLogger[ShardQuery, A] {
        val M = shardQueryMonad
      }
    }
  }

  protected def executor(implicit shardQueryMonad: ShardQueryMonad): QueryExecutor[ShardQuery, StreamT[ShardQuery, CharBuffer]]

  def executorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, StreamT[Future, CharBuffer]]]] = {
    import scalaz.syntax.monad._
    syncExecutorFor(apiKey) map { queryExecV =>
      queryExecV map { queryExec =>
        new QueryExecutor[Future, StreamT[Future, CharBuffer]] {
          def execute(query: String, context: EvaluationContext, opts: QueryOptions) = {
            queryExec.execute(query, context, opts) map (_ map (_._2))
          }
        }
      }
    }
  }

  def asyncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, JobId]]]
  def syncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, (Option[JobId], StreamT[Future, CharBuffer])]]]

  trait ManagedQueryExecutor[+A] extends QueryExecutor[Future, A] {
    import UserQuery.Serialization._

    implicit def executionContext: ExecutionContext
    implicit def futureMonad = new blueeyes.bkka.FutureMonad(executionContext)

    def complete(results: Future[Validation[EvaluationError, StreamT[ShardQuery, CharBuffer]]])(implicit
        M: ShardQueryMonad): Future[Validation[EvaluationError, A]]

    def execute(query: String, context: EvaluationContext, opts: QueryOptions): Future[Validation[EvaluationError, A]] = {
      val userQuery = UserQuery(query, context.basePath, opts.sortOn, opts.sortOrder)

      createJob(context.apiKey, Some(userQuery.serialize), opts.timeout)(executionContext) flatMap { implicit shardQueryMonad: ShardQueryMonad =>
        import JobQueryState._

        val result: Future[Validation[EvaluationError, StreamT[ShardQuery, CharBuffer]]] = {
          sink.apply(executor.execute(query, context, opts)) recover {
            case _: QueryCancelledException => Failure(InvalidStateError("Query was cancelled before it could be executed."))
            case _: QueryExpiredException => Failure(InvalidStateError("Query expired before it could be executed."))
            case ex =>
              System.out.println(">>> " + ex)
              System.err.println(">>> " + ex)
              throw ex
          }
        }

        complete(result)
      }
    }
  }

  trait SyncQueryExecutor extends ManagedQueryExecutor[(Option[JobId], StreamT[Future, CharBuffer])] {
    def complete(result: Future[Validation[EvaluationError, StreamT[ShardQuery, CharBuffer]]])(implicit
        M: ShardQueryMonad): Future[Validation[EvaluationError, (Option[JobId], StreamT[Future, CharBuffer])]] = {
      result map { _ map (M.jobId -> completeJob(_)) }
    }
  }

  trait AsyncQueryExecutor extends ManagedQueryExecutor[JobId] {
    private lazy val Utf8 = Charset.forName("UTF-8")
    private lazy val JSON = MimeTypes.application / MimeTypes.json

    // Encode a stream of CharBuffers using the specified charset.
    private def encodeCharStream(stream: StreamT[Future, CharBuffer], charset: Charset)(implicit M: Monad[Future]): StreamT[Future, Array[Byte]] = {
      val encoder = charset.newEncoder
      stream map { chars =>
        val buffer = encoder.encode(chars)
        chars.flip()

        val bytes = new Array[Byte](buffer.remaining())
        buffer.get(bytes)
        bytes
      }
    }

    def complete(resultV: Future[Validation[EvaluationError, StreamT[ShardQuery, CharBuffer]]])(implicit
        M: ShardQueryMonad): Future[Validation[EvaluationError, JobId]] = {
      M.jobId map { jobId =>
        resultV map (_ map { result =>
          jobManager.setResult(jobId, Some(JSON), encodeCharStream(completeJob(result), Utf8)) map {
            case Left(error) =>
              jobManager.abort(jobId, "Error occured while storing job results: " + error, yggConfig.clock.now())
            case Right(_) =>
              // This is "finished" by `completeJob`.
          }
          jobId
        })
      } getOrElse {
        Future(Failure(InvalidStateError("Jobs service is down; cannot execute asynchronous queries.")))
      }
    }
  }
}
