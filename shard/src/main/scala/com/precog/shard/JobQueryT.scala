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
  def jobId: Option[JobId]
  def resources: Set[QueryResources[_]]
}

object JobQueryState {
  case class Cancelled(jobId: Option[JobId]) extends JobQueryState[Nothing] {
    def resources: Set[QueryResources[_]] = Set.empty
    def value = None
  }

  case class Managed[A](jobId0: JobId, resources: Set[QueryResource[_]], value0: A) extends JobQueryState[A] {
    def jobId = Some(jobId0)
    def value = Some(value0)
  }

  case class Unmanaged[A](resources: Set[QueryResource[_]], value0: A) extends JobQueryState[A] {
    def jobId = None
    def value = Some(value0)
  }
}

trait JobQueryStateManager[M[+_]] extends QueryStateManager[JobQueryState] {
  import JobQueryState._

  def point[A](a: => A): JobQueryState[A] = Unmanaged(Set.empty, a)

  def map[A, B](fa: JobQueryState[A](f: A => B): JobQueryState[B] = fa match {
    case Managed(jobId, resources, value) => Managed(jobId, resources, f(value))
    case Unmanaged(resources, value) => Unmanaged(resources, f(value))
    case cancelled => cancelled
  }

  def bind[A, B](fa: JobQueryState[A])(f: A => JobQueryState[B]): JobQueryState[B] = {
    fa.value map f map {
      case Managed(jobId, resources, value) =>
        Managed(jobId, fa.resources ++ resources, value)
      case Unmanaged(resources, value) =>
        fa.jobId map (Managed(_, fa.resources ++ resources, value)) getOrElse Unmanaged(fa.resources ++ resources, value)
      case cancelled @ Cancelled(_) =>
        cancelled
    } getOrElse fa
  }

  protected def rootAPIKey: APIKey
  protected def jobManager: JobManager[M]
  def actorSystem: ActorSystem
  def pollFrequency: Duration

  private val rwlock = new ReentrantReadWriteLock()
  private val readLock = rwlock.readLock()
  private val writeLock = rwlock.writeLock()
  private val jobStatus: Map[JobId, Job] = Map.empty

  private def poll() {
    jobManager.listJobs(rootAPIKey) map { jobs =>
      val newStatus = jobs.groupBy(_.id)
      writeLock.lock()
      try {
        jobStatus = newStatus
      } finally {
        writeLock.unlock()
      }
    }
  }

  private def isCancelled(jobId: JobId): Boolean = {
    readLock.lock()
    val status = try {
      status = jobStatus get jobId
    } finally {
      readLock.unlock()
    }

    status map {
      case Job(_, _, _, _, Cancelled(_, _, _), _) => true
      case _ => false
    } getOrElse false
  }

  private val poller = actorSystem.scheduler.schedule(pollFrequency, pollFrequency) {
    poll()
  }

  def stop(): Unit = poller.cancel()

  def freeResources(resources: Set[QueryResource]) = resources foreach { _.close() }

  def isCancelled[A](q: JobQueryState[A]): Boolean = q match {
    case Cancelled(_) => true
    case Managed(jobId, _, _) => isCancelled(jobId)
    case Unmanaged(_, _) => false
  }

  def cancel[A](q: JobQueryState[A]): JobQueryState[Nothing] = q match {
    case cancelled @ Cancelled(_) =>
      cancelled
    case Managed(jobId, resources, _) =>
      freeResources(resources)
      Cancelled(Some(jobId))
    case Unmanaged(_, _) =>
      freeResources(resources)
      Cancelled(None)
  }
}

object JobQueryStateManager {
  def apply[M[+_]](jobManager0: JobManager[M]): JobQueryStateManager[M] = new JobQueryStateManager[M] {
    val jobManager = jobManager0
  }
}

