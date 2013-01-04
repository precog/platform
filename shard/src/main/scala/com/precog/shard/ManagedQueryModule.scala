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

trait ManagedQueryModuleConfig {
  def jobPollFrequency: Duration
  def clock: Clock
}

trait ManagedQueryModule extends YggConfigComponent {
  import scalaz.syntax.monad._
  import JobQueryState._

  type YggConfig <: ManagedQueryModuleConfig

  private implicit val jobActorSystem = ActorSystem("jobPollingActorSystem")
  def jobManager: JobManager[Future]

  trait ShardQueryMonad extends QueryTMonad[JobQueryState, Future] with QueryTHoist[JobQueryState]

  def createJob(apiKey: APIKey, asyncContext: ExecutionContext): EitherT[Future, String, ShardQueryMonad] = {
    val job = jobManager.createJob(apiKey, "Super-Awesome Shard Query", "shard-query", Some(yggConfig.clock.now()), None)
    EitherT.eitherT(for {
      queryStateManager <- job map { job => JobQueryStateManager(job.id) } recover { case _ => FakeJobQueryStateManager }
    } yield \/.right(new ShardQueryMonad {
      val Q: SwappableMonad[JobQueryState] = queryStateManager
      val M: Monad[Future] = new blueeyes.bkka.FutureMonad(asyncContext)
    }))
  }

  // This can be used when the Job service is down.
  final object FakeJobQueryStateManager extends JobQueryStateMonad {
    def isCancelled() = false
  }

  final case class JobQueryStateManager(jobId: JobId) extends JobQueryStateMonad {
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

