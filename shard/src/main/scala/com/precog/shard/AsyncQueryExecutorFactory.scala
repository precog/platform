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
