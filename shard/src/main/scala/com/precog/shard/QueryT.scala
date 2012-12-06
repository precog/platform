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

trait QueryState[+A] {
  def value: Option[A]
}

trait QueryStateManager[Q[+_] <: QueryState[_]] extends Monad[Q] {
  def poll[A](q: Q[A]): Q[A] = if (isCancelled(q)) cancel(q) else q

  def isCancelled[A](q: Q[A]): Boolean
  def cancel[A](q: Q[A]): Q[Nothing]
}

final case class QueryT[Q[+_] <: QueryState[_], M[+_], +A](stateM: M[Q[A]])(implicit Q: QueryStateManager[Q]) {
  import scalaz.syntax.monad._

  def isAborted: M[Boolean] = state map (_.value.isEmpty)

  def getOrElse[AA >: A](aa: => AA): M[AA] = state map (_.value getOrElse aa)

  def map[B](f: A => B)(implicit M: Monad[M]) = stateM map { state =>
    Q.poll(state) map f
  }

  def flatMap[B](f: A => QueryT[Q, M, B])(implicit M: Monad[M]) = QueryT(stateM map (Q.poll(_)) flatMap { state =>
    state.value map f map { _.stateM map (state *> _) } getOrElse M.point(state)
  })
}

trait QueryTCompanion[Q[+_] <: QueryState[_]] extends QueryTInstances[Q] {
  def apply[Q[+_] <: QueryState[_], M[+_], A](a: M[A])(implicit M: Functor[M], Q: QueryStateManager[Q]): QueryT[Q, M, A] = {
    QueryT(a map Q.point)
  }
}

private trait QueryTInstances1[Q[+_] <: QueryState[_]] {
  implicit def queryTFunctor[M[+_]](implicit M0: Monad[M]): Monad[({type λ[α] = QueryT[M, Q, α]})#λ] = new QueryTFunctor[M] {
    implicit def M: Monad[M] = M0
  }
}

private trait QueryTInstances0[Q[+_] <: QueryState[_]] extends QueryTInstances1[Q] {
  implicit def queryTPointed[M[+_]](implicit M0: Monad[M]): Pointed[({type λ[α] = QueryT[M, α]})#λ] = new QueryTPointed[M] {
    implicit def M: Monad[M] = M0
  }
}

private trait QueryTInstances[Q[+_] <: QueryState[_]] extends QueryTInstances0[Q] {
  implicit def queryTMonadTrans: Hoist[QueryT] = new QueryTHoist[Q] { }

  implicit def queryTMonad[M[+_]](implicit M0: Monad[M]): Monad[({type λ[α] = QueryT[Q, M, α]})#λ] = new QueryTMonad[Q, M] {
    implicit def M: Monad[M] = M0
  }
}

private trait QueryTFunctor[Q[+_] <: QueryState[_], M[+_]] extends Functor[({ type λ[α] = QueryT[Q, M, α] })#λ] {
  implicit def M: Monad[M]

  // Unfortunately, we need a Monad, even for this case.
  def map[A, B](ma: QueryT[Q, M, A])(f: A => B): QueryT[Q, M, B] = ma map f
}

private trait QueryTPointed[Q[+_] <: QueryState[_], M[+_]] extends Pointed[({ type λ[α] = QueryT[Q, M, α] })#λ] extends QueryTFunctor[Q, M] {
  def point[A](a: => A): QueryT[Q, M, A] = QueryT(M.point(a))
}

private trait QueryTMonad[Q[+_] <: QueryState[_], M[+_]] extends Monad[({ type λ[α] = QueryT[Q, M, α] })#λ] extends QueryTPointed[Q, M] {
  def bind[A, B](fa: QueryT[Q, M, A])(f: A => QueryT[Q, M, B]): QueryT[Q, M, B] = fa flatMap f
}

private trait QueryTHoist[Q[+_] <: QueryState[_]] extends Hoist[({ type λ[m, α] = QueryT[Q, m, α] })#λ] {
  def liftM[M[+_], A](ma: M[A])(implicit M: Monad[M]): QueryT[Q, M, A] = QueryT[Q, M, A](M.map[A, QueryState[A]](ma) { a =>
    QueryState(None, clock.nanoTime(), Some(a))
  }

  def hoist[M[+_]: Monad, N[+_]](f: M ~> N) = new (({ type λ[α] = QueryT[Q, M, α] })#λ ~> ({ type λ[α] = QueryT[Q, N, α] })#λ) {
    def apply[A](ma: QueryT[Q, M, A]): QueryT[Q, N, A] = QueryT(f(ma.state))
  }

  implicit def apply[M[+_] : Monad]: Monad[({type λ[α] = QueryT[Q, M, α]})#λ] = QueryT.queryTMonad[M]
}
