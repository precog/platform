package com.precog.shard

import com.precog.yggdrasil.YggConfigComponent

import com.precog.common.jobs._
import com.precog.common.security._

import blueeyes.util.Clock

import java.util.concurrent.locks.ReentrantReadWriteLock

import akka.dispatch.{ Future, ExecutionContext }
import akka.actor.ActorSystem
import akka.util.Duration

import scalaz._

/**
 * A `QueryCancelledException` is thrown in a `Future` to indicate that the
 * query terminated abnormally because it was cancelled from the outside.
 */
case class QueryCancelledException(msg: String) extends Exception(msg)

trait ManagedQueryModuleConfig {

  /** Determines how often the Jobs service is polled for status updates. */
  def jobPollFrequency: Duration

  def clock: Clock
}

/**
 * A managed query is a query that is run in the context of a job. This context
 * is modelled as a monad transformer. Basically, this gives the world outside
 * the monad the ability to affect the world inside of it. It is not pure, but
 * can be useful as it allows the query to be affected by cancellation requests,
 * for instance.
 *
 * The main idea is to run the query itself within a `ShardQuery[+_]`. The
 * monad instance for a `ShardQuery` is obtained through `createJob(...)`. This
 * is then stripped off by `completeJob(...)`.
 *
 * Note that if the job service is down, then a ShardQueryMonad will still be
 * returned, it just won't actually have a job associated with it and, thus,
 * cannot be cancelled.
 */
trait ManagedQueryModule extends YggConfigComponent {
  import scalaz.syntax.monad._
  import JobQueryState._

  type YggConfig <: ManagedQueryModuleConfig

  type ShardQuery[+A] = JobQueryT[Future, A]

  trait ShardQueryMonad extends QueryTMonad[JobQueryState, Future] with QueryTHoist[JobQueryState] {
    def jobId: Option[JobId]
  }

  private implicit val jobActorSystem = ActorSystem("jobPollingActorSystem")

  def jobManager: JobManager[Future]

  /**
   * Creates a new job with the given apiKey, and uses that to construct a
   * ShardQueryMonad that is bound to this job. The behaviour of the
   * ShardQueryMonad depends on the job state and allows computations running
   * within the monad to be cancelled, expired, etc.
   *
   * Queries that are run within the ShardQueryMonad should be completed with
   * `completeJob` to ensure the job is put into a terminal state when the
   * query completes.
   */
  def createJob(apiKey: APIKey, name: String)(implicit asyncContext: ExecutionContext): EitherT[Future, String, ShardQueryMonad] = {
    val futureJob = jobManager.createJob(apiKey, name, "shard-query", Some(yggConfig.clock.now()), None)
    EitherT.eitherT(for {
      job <- futureJob map { job => Some(job) } recover { case _ => None }
      queryStateManager = job map { job => JobQueryStateManager(job.id) } getOrElse FakeJobQueryStateManager
    } yield \/.right(new ShardQueryMonad {
      val jobId = job map (_.id)
      val Q: SwappableMonad[JobQueryState] = queryStateManager
      val M: Monad[Future] = new blueeyes.bkka.FutureMonad(asyncContext)
    }))
  }

  /**
   * Given the result of a managed query as a stream, this will ensure the job
   * is finished correctly. If the query is cancelled, then the job will be
   * aborted. If the stream is run to completion, then the job will be
   * finished.
   *
   * This also turns the result stream into a simple StreamT[Future, A], as
   * this method is essentially the sink for managed queries.
   */
  def completeJob[A](result: StreamT[ShardQuery, A])(implicit M: ShardQueryMonad): StreamT[Future, A] = {
    val stripShardQuery = implicitly[Hoist[StreamT]].hoist[ShardQuery, Future](new (ShardQuery ~> Future) {
      def apply[A](f: ShardQuery[A]): Future[A] = f.stateM map (_ getOrElse {
        M.jobId map (jobManager.abort(_, "Query was cancelled.", yggConfig.clock.now()))
        throw QueryCancelledException("Query was cancelled before it was completed!")
      })
    })

    val finish: StreamT[ShardQuery, A] = StreamT[ShardQuery, A](M.point(StreamT.Skip {
      M.jobId map (jobManager.finish(_, None, yggConfig.clock.now()))
      StreamT.empty[ShardQuery, A]
    }))

    stripShardQuery(result ++ finish)
  }

  // This can be used when the Job service is down.
  private final object FakeJobQueryStateManager extends JobQueryStateMonad {
    def isCancelled() = false
  }

  private final case class JobQueryStateManager(jobId: JobId) extends JobQueryStateMonad {
    import JobQueryState._

    private val rwlock = new ReentrantReadWriteLock()
    private val readLock = rwlock.readLock()
    private val writeLock = rwlock.writeLock()
    private var jobStatus: Option[Job] = None

    private def poll() {
      jobManager.findJob(jobId) map { job =>
        writeLock.lock()
        try {
          jobStatus = job
        } finally {
          writeLock.unlock()
        }
      }
    }

    def isCancelled(): Boolean = {
      readLock.lock()
      val status = try {
        jobStatus
      } finally {
        readLock.unlock()
      }

      import JobState._
      status map {
        case Job(_, _, _, _, Cancelled(_, _, _), _) => true
        case _ => false
      } getOrElse false
    }

    // TODO: Should this be explicitly started?
    private val poller = jobActorSystem.scheduler.schedule(yggConfig.jobPollFrequency, yggConfig.jobPollFrequency) {
      poll()
    }

    def stop(): Unit = poller.cancel()
  }
}

