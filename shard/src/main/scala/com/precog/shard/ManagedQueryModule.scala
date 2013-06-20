package com.precog.shard

import com.precog.yggdrasil.YggConfigComponent

import com.precog.common.jobs._
import com.precog.common.security._
import com.precog.daze.QueryLogger

import blueeyes.util.Clock
import blueeyes.json._

import com.weiglewilczek.slf4s.Logging

import java.util.concurrent.atomic.AtomicBoolean

import org.joda.time.DateTime

import akka.dispatch.{ Future, ExecutionContext }
import akka.actor.{ ActorSystem, Cancellable }
import akka.util.Duration

import scalaz._

/**
 * A `QueryCancelledException` is thrown in a `Future` to indicate that the
 * query terminated abnormally because it was cancelled from the outside.
 */
case class QueryCancelledException(msg: String) extends Exception(msg)

/**
 * A `QueryExpiredException` is thrown in a `Future` to indicate that the
 * query terminated abnormally because its allotted time had expired.
 */
case class QueryExpiredException(msg: String) extends Exception(msg)

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
 * cannot be cancelled (though timeouts still work fine).
 */
trait ManagedQueryModule extends YggConfigComponent with Logging {
  import scalaz.syntax.monad._
  import JobQueryState._

  type YggConfig <: ManagedQueryModuleConfig

  trait ShardQueryMonad extends QueryTMonad[JobQueryState, Future] with QueryTHoist[JobQueryState] {
    def jobId: Option[JobId]
    def Q: JobQueryStateMonad
  }

  def defaultTimeout: Duration

  /**
   * A mix-in for `QueryLogger`s that forcefully aborts a shard query on fatal
   * errors.
   */
  trait ShardQueryLogger[M[+_], P] extends QueryLogger[M, P] {
    def M: ShardQueryMonad

    abstract override def die(): M[Unit] = {
      M.Q.abort()
      super.die()
    }
  }

  implicit def jobActorSystem: ActorSystem

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
  def createJob(apiKey: APIKey, data: Option[JValue], timeout: Option[Duration])(implicit asyncContext: ExecutionContext): Future[ShardQueryMonad] = {
    val start = System.currentTimeMillis
    val futureJob = jobManager.createJob(apiKey, "Quirrel Query", "shard-query", data, Some(yggConfig.clock.now())).onComplete {
      _ => logger.debug("Job created in %d ms".format(System.currentTimeMillis - start))
    }
    for {
      job <- futureJob map { job => Some(job) } recover { case _ => None }
      queryStateManager = job map { job =>
        val mgr = JobQueryStateManager(job.id, yggConfig.clock.now() plus timeout.getOrElse(defaultTimeout).toMillis)
        mgr.start()
        mgr
      } getOrElse FakeJobQueryStateManager(yggConfig.clock.now() plus timeout.getOrElse(defaultTimeout).toMillis)
    } yield {
      new ShardQueryMonad {
        val jobId = job map (_.id)
        val Q: JobQueryStateMonad = queryStateManager
        val M: Monad[Future] = new blueeyes.bkka.FutureMonad(asyncContext)
      }
    }
  }

  /**
   * This acts as a sink for `ShardQuery`, turning it into a plain future. It
   * will deal with cancelled and expired queries by updating the job and
   * throwing either a `QueryCancelledException` or a `QueryExpiredException`.
   * If a value is successfully pulled out of `f`, then it will be returned.
   * However, `sink` will not mark the job as successful here. It onyl deals
   * with failures.
   */
  implicit def sink(implicit M: ShardQueryMonad) = new (ShardQuery ~> Future) {
    def apply[A](f: ShardQuery[A]): Future[A] = f.run recover {
        case ex =>
          M.jobId map { jobId =>
            jobManager.addMessage(jobId, JobManager.channels.ServerError, JString("Internal server error."))
            jobManager.abort(jobId, "Internal server error.", yggConfig.clock.now())
          }
          throw ex
      } map {
        case Running(_, value) =>
          value
        case Cancelled =>
          M.jobId map (jobManager.abort(_, "Query was cancelled.", yggConfig.clock.now()))
          throw QueryCancelledException("Query was cancelled before it was completed.")
        case Expired =>
          M.jobId map (jobManager.expire(_, yggConfig.clock.now()))
          throw QueryExpiredException("Query expired before it was completed.")
      }
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
  def completeJob[N[+_], A](result: StreamT[ShardQuery, A])(implicit M: ShardQueryMonad, t: ShardQuery ~> N): StreamT[N, A] = {
    val finish: StreamT[ShardQuery, A] = StreamT[ShardQuery, A](M.point(StreamT.Skip {
      M.jobId map (jobManager.finish(_, yggConfig.clock.now()))
      StreamT.empty[ShardQuery, A]
    }))

    implicitly[Hoist[StreamT]].hoist[ShardQuery, N](t).apply(result ++ finish)
  }

  // This can be used when the Job service is down.
  private final case class FakeJobQueryStateManager(expiresAt: DateTime) extends JobQueryStateMonad {
    private val cancelled: AtomicBoolean = new AtomicBoolean()
    def abort(): Boolean = { cancelled.set(true); true }
    def isCancelled() = cancelled.get()
    def hasExpired() = yggConfig.clock.now() isAfter expiresAt
  }

  private final case class JobQueryStateManager(jobId: JobId, expiresAt: DateTime) extends JobQueryStateMonad with Logging {
    import JobQueryState._

    private[this] val cancelled: AtomicBoolean = new AtomicBoolean()
    private[this] val lock = new AnyRef

    private def poll() = lock.synchronized {
      import JobState._

      jobManager.findJob(jobId) map { job =>
        if (job map (_.state.isTerminal) getOrElse true) {
          logger.debug("Terminal state for " + jobId)
          abort()
        } else if (hasExpired) {
          logger.debug("Expired job %s, stopping poll".format(jobId))
          stop()
        } else {
          // logger.debug("Non-Terminal state for " + jobId)
          // We only update cancelled if we have not yet cancelled.
          cancelled.compareAndSet(false, job map {
            case Job(_, _, _, _, _, Cancelled(_, _, _)) => true
            case _ => false
          } getOrElse false)
        }
      }
    }

    def abort(): Boolean = lock.synchronized {
      cancelled.set(true)
      stop()
      true
    }

    def hasExpired(): Boolean = {
      yggConfig.clock.now() isAfter expiresAt
    }

    def isCancelled(): Boolean = cancelled.get()

    private var poller: Option[Cancellable] = None

    def start(): Unit = lock.synchronized {
      if (poller.isEmpty) {
        poller = Some(jobActorSystem.scheduler.schedule(yggConfig.jobPollFrequency, yggConfig.jobPollFrequency) {
          poll()
        })
      }
    }

    def stop(): Unit = lock.synchronized {
      logger.debug("Stopping scheduled poll for " + jobId)
      poller foreach {
        c => c.cancel(); logger.debug("Cancelled %s: %s".format(jobId, c.isCancelled))
      }
      poller = None
    }
  }
}
