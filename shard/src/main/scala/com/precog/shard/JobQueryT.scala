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

import java.util.concurrent.locks.ReadWriteLock

case class QueryResource[A](a: A, closeable: Closeable[A]) {
  def close(): M[PrecogUnit] closeable.close(a)
}

sealed trait JobQueryState[+A] extends QueryState[A] {
  import JobQueryState._

  def running[B](f: (Set[QueryResource[_]], A) => B): Option[B] = this match {
    case Cancelled => None
    case Running(resources, value) = Some(f(resources, value))
  }
}

object JobQueryState {
  case object Cancelled extends JobQueryState[Nothing] { def value = None }
  case class Running[A](resources: Set[QueryResource[_]], value0: A) extends JobQueryState[A] {
    def value = Some(value0)
  }
}

trait JobQueryStateMonad extends Monad[JobQueryState] {
  def point[A](a: => A): JobQueryState[A] = Running(Set.empty, a)

  def map[A, B](fa: JobQueryState[A])(f: A => B): JobQueryState[B] = fa match {
    case Running(resources, value) => Running(resources, f(value))
    case Cancelled => Cancelled
  }

  def bind[A, B](fa: JobQueryState[A])(f: A => JobQueryState[B]): JobQueryState[B] = fa match {
    case Running(resources0, value0) => f(value0) match {
      case Running(resources1, value) => Running(resources0 ++ resources1, value)
      case Cancelled => Cancelled
    }
    case Cancelled => Cancelled
  }
}


trait ManagedQueryModule[M[+_]] {
  def actorSystem: ActorSystem
  def jobManager: JobManager[M]

  // This can be used when the Job service is down.
  final object FakeJobQueryStateManager extends QueryStateManager[JobQueryState] with JobQueryStateMonad {
    def isCancelled[A](q: JobQueryState[A]): Boolean = q match
      case Running(_, _) => false
      case Cancelled => true
    }

    def cancel[A](q: JobQueryState[A]): JobQueryState[Nothing] = Cancelled
  }

  final case class JobQueryStateManager[M[+_]](jobId: JobId) extends QueryStateManager[JobQueryState] with JobQueryStateMonad {
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

    private def isCancelled(): Boolean = {
      readLock.lock()
      val status = try {
        jobStatus
      } finally {
        readLock.unlock()
      }

      status map {
        case Job(_, _, _, _, Cancelled(_, _, _), _) => true
        case _ => false
      } getOrElse false
    }

    private def freeResources(resources: Set[QueryResource]) = resources foreach { _.close() }

    def isCancelled[A](q: JobQueryState[A]): Boolean =  q.running { (_, _) => isCancelled() } getOrElse true

    def cancel[A](q: JobQueryState[A]): JobQueryState[Nothing] = {
      q.running { (resources, _) => freeResources(resources) }
      Cancelled
    }

    // TODO: Should this be explicitly started?
    private val poller = actorSystem.scheduler.schedule(pollFrequency, pollFrequency) {
      poll()
    }

    def stop(): Unit = poller.cancel()

    // Monad implementation.

    def point[A](a: => A): JobQueryState[A] = Running(Set.empty, a)

    def map[A, B](fa: JobQueryState[A])(f: A => B): JobQueryState[B] = fa match {
      case Running(resources, value) => Running(resources, f(value))
      case Cancelled => Cancelled
    }

    def bind[A, B](fa: JobQueryState[A])(f: A => JobQueryState[B]): JobQueryState[B] = fa match {
      case Running(resources0, value0) => f(value0) match {
        case Running(resources1, value) => Running(resources0 ++ resources1, value)
        case Cancelled => Cancelled
      }
      case Cancelled => Cancelled
    }
  }
}

