package com.precog.shard

import com.precog.yggdrasil.YggConfigComponent
import com.precog.common.jobs._

import blueeyes.util.Close

import java.util.concurrent.locks.ReentrantReadWriteLock

import akka.dispatch.Future
import akka.actor.ActorSystem
import akka.util.Duration

import scalaz._

case class QueryResource[A](a: A, close0: Close[A]) {
  def close(): Unit = close0.close(a)
}

sealed trait JobQueryState[+A] {
  import JobQueryState._

  def getOrElse[AA >: A](aa: => AA) = this match {
    case Cancelled => aa
    case Running(_, value) => value
  }
}

object JobQueryState {
  case object Cancelled extends JobQueryState[Nothing] { def value = None }
  case class Running[A](resources: Set[QueryResource[_]], value0: A) extends JobQueryState[A] {
    def value = Some(value0)
  }
}

trait JobQueryStateMonad extends SwappableMonad[JobQueryState] {
  import JobQueryState._

  def isCancelled(): Boolean

  def swap[M[+_], A](state: JobQueryState[M[A]])(implicit M: Monad[M]): M[JobQueryState[A]] = {
    state match {
      case Running(resources, ma) => M.map(ma)(Running(resources, _))
      case Cancelled => M.point(Cancelled)
    }
  }

  def point[A](a: => A): JobQueryState[A] = if (isCancelled()) Cancelled else Running(Set.empty, a)

  def maybeCancel[A](q: JobQueryState[A]): JobQueryState[A] = if (isCancelled()) {
    // Free resources from q.
    Cancelled
  } else {
    q
  }

  override def map[A, B](fa: JobQueryState[A])(f: A => B): JobQueryState[B] = maybeCancel(fa) match {
    case Running(resources, value) => Running(resources, f(value))
    case Cancelled => Cancelled
  }

  def bind[A, B](fa: JobQueryState[A])(f: A => JobQueryState[B]): JobQueryState[B] = maybeCancel(fa) match {
    case Running(resources0, value0) => f(value0) match {
      case Running(resources1, value) => Running(resources0 ++ resources1, value)
      case Cancelled => Cancelled
    }
    case Cancelled => Cancelled
  }
}


trait ManagedQueryModuleConfig {
  def jobPollFrequency: Duration
}

trait ManagedQueryModule extends YggConfigComponent {
  import scalaz.syntax.monad._
  import JobQueryState._

  type YggConfig <: ManagedQueryModuleConfig

  private implicit val jobActorSystem = ActorSystem("jobPollingActorSystem")
  // private implicit val futureFunctor: Functor[M] = new blueeyes.bkka.FutureMonad(ExecutionContext.defaultExecutionContext(actorSystem))
  def jobManager: JobManager[Future]

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

