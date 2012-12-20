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

trait AsyncQueryExecutorFactory { self: ManagedQueryModule =>

  def asyncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, JobId]]]

  trait AsyncQueryExecutor extends QueryExecutor[Future, JobId] {
    private final val charset = Charset.forName("UTF-8")

    implicit def executionContext: ExecutionContext
    def executor(implicit shardQueryMonad: ShardQueryMonad): QueryExecutor[ShardQuery, StreamT[ShardQuery, CharBuffer]]
    
    // Encode a stream of CharBuffers using the specified charset.
    private def encodeCharStream(stream: StreamT[Future, CharBuffer], charset: Charset)(implicit M: Monad[Future]): StreamT[Future, Array[Byte]] = {
      val encoder = charset.newEncoder
      stream map { chars =>
        val buffer = encoder.encode(chars)
        try {
          buffer.array()
        } catch {
          case (_: ReadOnlyBufferException) | (_: UnsupportedOperationException) =>
            // This won't happen normally.
            val bytes = new Array[Byte](buffer.remaining())
            buffer.get(bytes)
            bytes
        }
      }
    }

    private val Utf8 = Charset.forName("UTF-8")
    private val JSON = MimeTypes.application / MimeTypes.json

    def execute(apiKey: String, query: String, prefix: Path, opts: QueryOptions) = {
      import UserQuery.Serialization._

      val userQuery = UserQuery(query, prefix, opts.sortOn, opts.sortOrder)
      val expires = opts.timeout map (yggConfig.clock.now().plus(_))
      createJob(apiKey, Some(userQuery.serialize), expires)(executionContext) flatMap { implicit shardQueryMonad: ShardQueryMonad =>
        import JobQueryState._

        val resultV: Future[Validation[EvaluationError, StreamT[ShardQuery, CharBuffer]]] = {
          sink.apply(executor.execute(apiKey, query, prefix, opts)) recover {
            case _: QueryCancelledException => Failure(InvalidStateError("Query was cancelled before it could be executed."))
            case _: QueryExpiredException => Failure(InvalidStateError("Query expired before it could be executed."))
          }
        }

        shardQueryMonad.jobId map { jobId =>
          resultV map (_ map { result =>
            jobManager.setResult(jobId, Some(JSON), encodeCharStream(completeJob(result), Utf8)(shardQueryMonad.M)) map {
              case Left(error) => sys.error("Failed to create results... what to do.") // FIXME: Yep.
              case Right(_) => // Yay!
            }
            jobId
          })
        } getOrElse {
          Future(Failure(InvalidStateError("Jobs service is down; cannot execute asynchronous queries.")))
        }
      }
    }
  }
}
