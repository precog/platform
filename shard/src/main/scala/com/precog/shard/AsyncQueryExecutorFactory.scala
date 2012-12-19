package com.precog.shard

import com.precog.daze._

import com.precog.util._
import com.precog.common._
import com.precog.common.security._
import com.precog.common.jobs._

import akka.dispatch.{ Future, ExecutionContext }

import java.nio.charset._
import java.nio.channels.WritableByteChannel
import java.nio.{ CharBuffer, ByteBuffer }
import java.io.{ File, FileOutputStream }

import blueeyes.json.serialization.DefaultSerialization.{ DateTimeExtractor => _, DateTimeDecomposer => _, _ }

import scalaz._

trait AsyncQueryExecutorFactory { self: ManagedQueryModule =>

  def asyncExecutorFor(apiKey: APIKey): Future[Validation[String, QueryExecutor[Future, JobId]]]

  trait AsyncQueryExecutor extends QueryExecutor[Future, JobId] {
    private final val charset = Charset.forName("UTF-8")

    implicit def executionContext: ExecutionContext
    def executor(implicit shardQueryMonad: ShardQueryMonad): QueryExecutor[ShardQuery, StreamT[ShardQuery, CharBuffer]]

    // def encodeCharStream(stream: StreamT[ShardQuery, CharBuffer]): StreamT[Future, Array[Byte]]

    // Writes result to the byte channel.
    private def storeQuery(result: StreamT[ShardQuery, CharBuffer])(implicit M: ShardQueryMonad): Future[PrecogUnit] = {
      import JobQueryState._

      //M.jobId map { jobId =>
      //  completeJob(result)
      //  jobManager.setResult(encodeCharStream(result)) map {

      val file = new File("ZOMG!") // createQueryResult(M.jobId)
      val channel = new FileOutputStream(file).getChannel()
      val encoder = charset.newEncoder
      val buffer = ByteBuffer.allocate(1024 * 50)

      def loop(result: StreamT[ShardQuery, CharBuffer]): Future[PrecogUnit] = result.uncons.run flatMap {
        case Running(_, Some((chars, tail))) =>
          while (encoder.encode(chars, buffer, false) != CoderResult.UNDERFLOW) {
            while (buffer.remaining() > 0) {
              channel.write(buffer)
            }
            buffer.clear()
          }
          loop(tail)

        case Cancelled =>
          channel.close()
          M.jobId map (jobManager.abort(_, "Query was cancelled.", yggConfig.clock.now()) map { _ => PrecogUnit }) getOrElse Future(PrecogUnit)

        case Expired =>
          channel.close()
          M.jobId map (jobManager.expire(_, yggConfig.clock.now()) map { _ => PrecogUnit }) getOrElse Future(PrecogUnit)

        case Running(_, None) =>
          channel.close()
          M.jobId map (jobManager.finish(_, yggConfig.clock.now()) map { _ => PrecogUnit }) getOrElse Future(PrecogUnit)
      }

      loop(result)
    }

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

        resultV map (_ map { stream =>
          storeQuery(stream)
          shardQueryMonad.jobId.get // FIXME: FIXME: FIXME: Yep.
        })
      }
    }
  }

}

