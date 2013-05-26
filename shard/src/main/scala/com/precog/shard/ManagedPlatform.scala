package com.precog.shard

import com.precog.daze._

import com.precog.util._
import com.precog.common._
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.yggdrasil._
import com.precog.yggdrasil.execution._
import com.precog.yggdrasil.table._
import com.precog.util._

import akka.dispatch.{ Future, ExecutionContext }

import org.joda.time.DateTime

import java.nio.charset._
import java.nio.channels.WritableByteChannel
import java.nio.{ CharBuffer, ByteBuffer, ReadOnlyBufferException }
import java.io.{ File, FileOutputStream }

import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }
import blueeyes.core.http.MimeType
import blueeyes.core.http.MimeTypes

import scalaz._

trait ManagedPlatform extends ManagedExecution with Platform[Future, Slice, StreamT[Future, Slice]] with ManagedQueryModule

/**
 * A `ManagedPlatform` extends `Platform` by allowing the
 * creation of a `BasicQueryExecutor` that can also allows "synchronous" (but
 * managed) queries and asynchronous queries.
 */
trait ManagedExecution extends Execution[Future, StreamT[Future, Slice]] with ManagedQueryModule { self =>
  type AsyncExecution[M[+_]] = Execution[M, JobId]
  type SyncExecution[M[+_]]  = Execution[M, (Option[JobId], StreamT[Future, Slice])]

  /**
   * Returns an `Execution` whose execution returns a `JobId` rather
   * than a `StreamT[Future, Slice]`.
   */
  def asynchronous: AsyncExecution[Future] = {
    new AsyncExecution[Future] {
      def executorFor(apiKey: APIKey) = self.asyncExecutorFor(apiKey)
    }
  }

  /**
   * Returns a `Execution` whose execution returns both the
   * streaming results and its `JobId`. Note that the reults will not be saved
   * to job.
   */
  def synchronous: SyncExecution[Future] = {
    new SyncExecution[Future] {
      def executorFor(apiKey: APIKey) = self.syncExecutorFor(apiKey)
    }
  }

  def errorReport[A](implicit shardQueryMonad: JobQueryTFMonad, decomposer0: Decomposer[A]): QueryLogger[JobQueryTF, A] = {
    import scalaz.syntax.monad._

    implicit val M = shardQueryMonad.M

    shardQueryMonad.jobId map { jobId0 =>
      val lift = new (Future ~> JobQueryTF) {
        def apply[A](fa: Future[A]) = fa.liftM[JobQueryT]
      }

      new JobQueryLogger[JobQueryTF, A] with ShardQueryLogger[JobQueryTF, A] with TimingQueryLogger[JobQueryTF, A] {
        val M = shardQueryMonad
        val jobManager = self.jobManager.withM[JobQueryTF](lift, implicitly, shardQueryMonad.M, shardQueryMonad)
        val jobId = jobId0
        val clock = yggConfig.clock
        val decomposer = decomposer0
      }
    } getOrElse {
      new LoggingQueryLogger[JobQueryTF, A] with ShardQueryLogger[JobQueryTF, A] with TimingQueryLogger[JobQueryTF, A] {
        val M = shardQueryMonad
      }
    }
  }

  protected def executor(implicit shardQueryMonad: JobQueryTFMonad): QueryExecutor[JobQueryTF, StreamT[JobQueryTF, Slice]]

  def executorFor(apiKey: APIKey): EitherT[Future, String, QueryExecutor[Future, StreamT[Future, Slice]]] = {
    import scalaz.syntax.monad._
    for (queryExec <- syncExecutorFor(apiKey)) yield {
      new QueryExecutor[Future, StreamT[Future, Slice]] {
        def execute(query: String, context: EvaluationContext, opts: QueryOptions) = {
          queryExec.execute(query, context, opts) map { _._2 }
        }
      }
    }
  }

  def asyncExecutorFor(apiKey: APIKey): EitherT[Future, String, QueryExecutor[Future, JobId]]

  def syncExecutorFor(apiKey: APIKey): EitherT[Future, String, QueryExecutor[Future, (Option[JobId], StreamT[Future, Slice])]]

  trait ManagedQueryExecutor[+A] extends QueryExecutor[Future, A] {
    import UserQuery.Serialization._

    implicit def executionContext: ExecutionContext
    implicit def futureMonad = new blueeyes.bkka.FutureMonad(executionContext)

    def complete(results: EitherT[Future, EvaluationError, StreamT[JobQueryTF, Slice]], outputType: MimeType)(implicit M: JobQueryTFMonad): EitherT[Future, EvaluationError, A]

    def execute(query: String, context: EvaluationContext, opts: QueryOptions): EitherT[Future, EvaluationError, A] = {
      val userQuery = UserQuery(query, context.basePath, opts.sortOn, opts.sortOrder)

      //TODO: this is craziness
      EitherT.right(createQueryJob(context.apiKey, Some(userQuery.serialize), opts.timeout)(executionContext)) flatMap { implicit shardQueryMonad: JobQueryTFMonad =>
        import JobQueryState._

        complete(EitherT.eitherTHoist[EvaluationError].hoist(sink).apply(executor.execute(query, context, opts)), opts.output)
      }
    }
  }

  trait SyncQueryExecutor extends ManagedQueryExecutor[(Option[JobId], StreamT[Future, Slice])] {
    def complete(result: EitherT[Future, EvaluationError, StreamT[JobQueryTF, Slice]], outputType: MimeType)(implicit M: JobQueryTFMonad): EitherT[Future, EvaluationError, (Option[JobId], StreamT[Future, Slice])] = {
      result map { stream =>
        M.jobId -> completeJob(stream)
      }
    }
  }

  trait AsyncQueryExecutor extends ManagedQueryExecutor[JobId] {
    private lazy val Utf8 = Charset.forName("UTF-8")
    private lazy val JSON = MimeTypes.application / MimeTypes.json

    // Encode a stream of Slices using the specified charset.
    //FIXME: replace with VFSModule.bufferOutput?
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

    def complete(resultE: EitherT[Future, EvaluationError, StreamT[JobQueryTF, Slice]], outputType: MimeType)(implicit M: JobQueryTFMonad): EitherT[Future, EvaluationError, JobId] = {
      M.jobId map { jobId =>
        resultE map { result =>
          val derefed = result.map(_.deref(TransSpecModule.paths.Value))
          val convertedStream: StreamT[JobQueryTF, CharBuffer] = ColumnarTableModule.toCharBuffers(outputType, derefed)
          //FIXME: Thread this through the EitherT
          jobManager.setResult(jobId, Some(JSON), encodeCharStream(completeJob(convertedStream), Utf8)) map {
            case Left(error) =>
              jobManager.abort(jobId, "Error occured while storing job results: " + error, yggConfig.clock.now())
            case Right(_) =>
              // This is "finished" by `completeJob`.
          }

          jobId
        }
      } getOrElse {
        EitherT.left(Future(InvalidStateError("Jobs service is down; cannot execute asynchronous queries.")))
      }
    }
  }
}
