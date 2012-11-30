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

import scalaz._

private final case class JobInfo[M[+_]](jobManager: JobManager[M], jobId: JobId, pollingInterval: Long)

private final case class QueryState[M[+_], A](jobInfo: Option[JobInfo[M]], lastPolled: Long, value: Option[A])

final case class QueryT[M[+_], +A](state: M[QueryState[M, A]]) {
  import scalaz.syntax.monad._

  def getJobId: M[Option[JobId]] = state map (_.job map (_._2))

  def isAborted: M[Boolean] = state map (_.value.isEmpty)

  def getOrElse[AA >: A](aa: => AA): M[AA] = state map (_.value getOrElse aa)

  def map[B](f: A => B)(implicit M: Monad[M]) = QueryT(poll map { newState =>
    newState.copy(value = value map f)
  })

  def flatMap[B](f: A => QueryT[B])(implicit M: Monad[M]) = QueryT(poll flatMap {
    case orig @ QueryState(jobId0, lastPoll0, value0) =>
      value0 map f map { _.state map { case QueryState(jobId, lastPoll, value) =>
        QueryState(jobId orElse jobId0, lastPoll, value)
      } } getOrElse orig
  })

  // Still need to implement type class.
  def traverse[N[_], B](f: A => N[B])(implicit M: Traverse[M], N: Applicative[N]): N[QueryT[M, B]] = {
    N.map(M.traverse(state) { case QueryState(jobId, lastPolled, value0) =>
      N.map(Traverse[Option].traverse(value0)(f))(QueryState(jobId, lastPolled, _))
    })(QueryT(_))
  }

  private def isCancelled(implicit M: Pointed[M]): M[Boolean] = jobInfo map { case JobInfo(jobManager, jobId, _) =>
    f(jobManager.getJob(jobId) map {
      case Some(Job(_, _, Cancelled(_, _, _), _)) => true
      case _ => false
    })
  } getOrElse M.point(false)

  private def poll(implicit M: Monad): M[QueryState[M, A]] = state flatMap {
    case QueryState(None, _, _) | QueryState(_, _, None) =>
      state

    case QueryState(Some(JobInfo(_, jobId, interval), lastPoll, Some(a)) =>
      val curPoll = clock.nanoTime() / 1000
      val duration = curPoll - lastPoll
      if (duration < interval) {
        state
      } else {
        isCancelled map { cancelled =>
          QueryState(Some(job), curPoll, cancelled.option(a))
        }
      }
  }
}

object QueryT with QueryTInstances {
  val DefaultPollingInterval = 2000 // ms

  def apply[M[+_], A](a: M[A]): QueryT[M, A] = QueryT(a map { a => (None, clock.nanoTime(), Some(a)) })

  def createJob[M[+_], N[+_]](jobManager: JobManager[N], apiKey: APIKey, pollingInterval: Long = DefaultPollingInterval)(implicit N: Functor[N], f: N ~> M): N[QueryT[M, Job]] = {
    import scalaz.syntax.functor._
    QueryT(f(jobManager.createJob(apiKey) map { job =>
      QueryState(Some(jobManager, job.id), clock.nanoTime(), Some(job))
    }))
  }
}

private trait QueryTInstances1 {
  implicit def queryTFunctor[M[+_]](implicit M0: Monad[M]): Monad[({type λ[α] = QueryT[M, α]})#λ] = new QueryTFunctor[M] {
    implicit def M: Monad[M] = M0
  }
}

private trait QueryTInstances0 extends QueryTInstances1 {
  implicit def queryTPointed[M[+_]](implicit M0: Monad[M]): Pointed[({type λ[α] = QueryT[M, α]})#λ] = new QueryTPointed[M] {
    implicit def M: Monad[M] = M0
  }
}

private trait QueryTInstances extends QueryTInstances0 {
  implicit def queryTMonadTrans: Hoist[QueryT] = new QueryTHoist {}

  implicit def queryTMonad[M[+_]](implicit M0: Monad[M]): Monad[({type λ[α] = QueryT[M, α]})#λ] = new QueryTMonad[M] {
    implicit def M: Monad[M] = M0
  }
}

private trait QueryTFunctor[M[+_]] extends Functor[({ type λ[α] = QueryT[M, α] })#λ] {
  implicit def M: Monad[M]

  // Unfortunately, we need a Monad, even for this case.
  def map[A, B](ma: QueryT[M, A])(f: A => B): QueryT[M, B] = ma map f
}

private trait QueryTPointed[M[+_]] extends Pointed[({ type λ[α] = QueryT[M, α] })#λ] extends QueryTFunctor[M] {
  def point[A](a: => A): QueryT[M, A] = QueryT(M.point(a))
}

private trait QueryTMonad[M[+_]] extends Monad[({ type λ[α] = QueryT[M, α] })#λ] extends QueryTPointed[M] {
  def bind[A, B](fa: QueryT[M, A])(f: A => QueryT[M, B]): QueryT[M, B] = fa flatMap f
}

private trait QueryTHoist extends Hoist[QueryT] {
  def liftM[M[+_], A](ma: M[A])(implicit M: Monad[M]): QueryT[M, A] = QueryT[M, A](M.map[A, QueryState[A]](ma) { a =>
    QueryState(None, clock.nanoTime(), Some(a))
  }

  def hoist[M[+_]: Monad, N[+_]](f: M ~> N) = new (({ type λ[α] = QueryT[M, α] })#λ ~> ({ type λ[α] = QueryT[N, α] })#λ) {
    def apply[A](ma: QueryT[M, A]): QueryT[N, A] = QueryT(f(ma.state))
  }

  implicit def apply[M[+_] : Monad]: Monad[({type λ[α] = QueryT[M, α]})#λ] = QueryT.queryTMonad[M]
}
