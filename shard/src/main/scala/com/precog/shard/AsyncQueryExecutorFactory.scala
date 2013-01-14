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

import akka.dispatch.{ Future, ExecutionContext }

import java.nio.charset._
import java.nio.channels.WritableByteChannel
import java.nio.{ CharBuffer, ByteBuffer, ReadOnlyBufferException }
import java.io.{ File, FileOutputStream }

import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }
import blueeyes.core.http.MimeTypes

import scalaz._

/**
 * An `AsyncQueryExecutorFactory` extends `QueryExecutorFactory` by allowing the
 * creation of a `QueryExecutor` that returns `JobId`s, rather than a stream of
 * results. It also has default implementable traits for both synchronous and
 * asynchronous QueryExecutors.
 */
trait AsyncQueryExecutorFactory extends QueryExecutorFactory[Future, StreamT[Future, CharBuffer]] { self: ManagedQueryModule =>

  /**
   * Returns an `QueryExecutorFactory` whose execution returns a `JobId` rather
   * than a `StreamT[Future, CharBuffer]`.
   */
  def asynchronous: QueryExecutorFactory[Future, JobId] = new QueryExecutorFactory[Future, JobId] {
    def browse(apiKey: APIKey, path: Path) = self.browse(apiKey, path)
    def structure(apiKey: APIKey, path: Path) = self.structure(apiKey, path)
    def executorFor(apiKey: APIKey) = self.asyncExecutorFor(apiKey)
    def status() = self.status()
    def startup() = self.startup()
    def shutdown() = self.shutdown()
  }

  def errorReport(implicit shardQueryMonad: ShardQueryMonad): ErrorReport[ShardQuery] = {
    import scalaz.syntax.monad._

    implicit val M0 = shardQueryMonad.M

    shardQueryMonad.jobId map { jobId0 =>
      val lift = new (Future ~> ShardQuery) {
        def apply[A](fa: Future[A]) = fa.liftM[JobQueryT]
      }

      new JobErrorReport[ShardQuery] {
        val M = shardQueryMonad
        val jobManager = self.jobManager.withM[ShardQuery](lift, implicitly, shardQueryMonad.M, shardQueryMonad)
        val jobId = jobId0
        val clock = yggConfig.clock
      }
    } getOrElse {
      new LoggingErrorReport[ShardQuery]
    }
  }

  protected def executor(implicit shardQueryMonad: ShardQueryMonad): QueryExecutor[ShardQuery, StreamT[ShardQuery, CharBuffer]]

  def asyncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, JobId]]]

  trait ManagedQueryExecutor[+A] extends QueryExecutor[Future, A] {
    import UserQuery.Serialization._

    implicit def executionContext: ExecutionContext
    implicit def futureMonad = new blueeyes.bkka.FutureMonad(executionContext)

    def complete(results: Future[Validation[EvaluationError, StreamT[ShardQuery, CharBuffer]]])(implicit
        M: ShardQueryMonad): Future[Validation[EvaluationError, A]]

    def execute(apiKey: String, query: String, prefix: Path, opts: QueryOptions): Future[Validation[EvaluationError, A]] = {
      val userQuery = UserQuery(query, prefix, opts.sortOn, opts.sortOrder)
      val expires = opts.timeout map (yggConfig.clock.now().plus(_))
      createJob(apiKey, Some(userQuery.serialize), expires)(executionContext) flatMap { implicit shardQueryMonad: ShardQueryMonad =>
        import JobQueryState._

        val result: Future[Validation[EvaluationError, StreamT[ShardQuery, CharBuffer]]] = {
          sink.apply(executor.execute(apiKey, query, prefix, opts)) recover {
            case _: QueryCancelledException => Failure(InvalidStateError("Query was cancelled before it could be executed."))
            case _: QueryExpiredException => Failure(InvalidStateError("Query expired before it could be executed."))
          }
        }

        complete(result)
      }
    }
  }

  trait SyncQueryExecutor extends ManagedQueryExecutor[StreamT[Future, CharBuffer]] {
    def complete(result: Future[Validation[EvaluationError, StreamT[ShardQuery, CharBuffer]]])(implicit
        M: ShardQueryMonad): Future[Validation[EvaluationError, StreamT[Future, CharBuffer]]] = {
      result map { _ map (completeJob(_)) }
    }
  }

  trait AsyncQueryExecutor extends ManagedQueryExecutor[JobId] {
    private val Utf8 = Charset.forName("UTF-8")
    private val JSON = MimeTypes.application / MimeTypes.json

    // Encode a stream of CharBuffers using the specified charset.
    private def encodeCharStream(stream: StreamT[Future, CharBuffer], charset: Charset)(implicit M: Monad[Future]): StreamT[Future, Array[Byte]] = {
      val encoder = charset.newEncoder
      stream map { chars =>
        val buffer = encoder.encode(chars)
        chars.flip()
        try {
          buffer.array()
        } catch {
          case (_: ReadOnlyBufferException) | (_: UnsupportedOperationException) =>
            // This won't happen normally, but should handled in any case.
            val bytes = new Array[Byte](buffer.remaining())
            buffer.get(bytes)
            bytes
        }
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
