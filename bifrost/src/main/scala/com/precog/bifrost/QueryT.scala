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

trait SwappableMonad[Q[+_]] extends Monad[Q] {
  def swap[M[+_], A](qmb: Q[M[A]])(implicit M: Monad[M]): M[Q[A]]
}

/**
 * So, through a series of incremental changes, QueryT has now becomes a generic
 * monad transformer constructor for monads that can flip themselves from the outside
 * in. For example, we could easily define:
 *
 * {{{
 * type OptionT[M, A] = QueryT[Option, M, A]
 *
 * implicit object SwappableMonad[Option] extends OptionMonad {
 *   def swap[M[+_], B](opt: Option[M[B]])(...) =
 *     opt map (_ map Some(_)) getOrElse M.point(None)
 * }
 * }}}
 */
final case class QueryT[Q[+_], M[+_], +A](run: M[Q[A]]) {
  import scalaz.syntax.monad._

  def map[B](f: A => B)(implicit M: Functor[M], Q: Functor[Q]): QueryT[Q, M, B] = QueryT(run map { _ map f })

  def flatMap[B](f: A => QueryT[Q, M, B])(implicit M: Monad[M], Q: SwappableMonad[Q]): QueryT[Q, M, B] = {
    QueryT(run flatMap { (state0: Q[A]) =>
      Q.swap(state0 map f map (_.run)) map { _ flatMap identity }
    })
  }
}

trait QueryTCompanion[Q[+_]] extends QueryTInstances[Q] with QueryTHoist[Q] {
  def apply[Q[+_], M[+_], A](a: M[A])(implicit M: Functor[M], Q: SwappableMonad[Q]): QueryT[Q, M, A] = {
    QueryT(M.map(a)(Q.point(_)))
  }
}

trait QueryTInstances0[Q[+_]] {
  implicit def queryTFunctor[M[+_]](implicit M0: Functor[M], Q0: Functor[Q]): Functor[({type λ[α] = QueryT[Q, M, α]})#λ] = new QueryTFunctor[Q, M] {
    def Q = Q0
    def M = M0
  }
}

trait QueryTInstances[Q[+_]] extends QueryTInstances0[Q] {
  implicit def queryTMonadTrans(implicit Q0: SwappableMonad[Q]): Hoist[({ type λ[μ[+_], α] = QueryT[Q, μ, α] })#λ] = new QueryTHoist[Q] {
    def Q = Q0
  }


  implicit def queryTMonad[M[+_]](implicit M0: Monad[M], Q0: SwappableMonad[Q]): Monad[({type λ[α] = QueryT[Q, M, α]})#λ] = new QueryTMonad[Q, M] {
    def Q = Q0
    def M = M0
  }
}

trait QueryTFunctor[Q[+_], M[+_]] extends Functor[({ type λ[α] = QueryT[Q, M, α] })#λ] {
  implicit def M: Functor[M]
  implicit def Q: Functor[Q]

  def map[A, B](ma: QueryT[Q, M, A])(f: A => B): QueryT[Q, M, B] = ma map f
}

trait QueryTMonad[Q[+_], M[+_]] extends Monad[({ type λ[α] = QueryT[Q, M, α] })#λ] with QueryTFunctor[Q, M] {
  implicit def M: Monad[M]
  implicit def Q: SwappableMonad[Q]

  def point[A](a: => A): QueryT[Q, M, A] = QueryT(M.point(Q.point(a)))
  def bind[A, B](fa: QueryT[Q, M, A])(f: A => QueryT[Q, M, B]): QueryT[Q, M, B] = fa flatMap f
  override def map[A, B](ma: QueryT[Q, M, A])(f: A => B): QueryT[Q, M, B] = super.map(ma)(f)
  override def ap[A, B](ma: => QueryT[Q, M, A])(mf: => QueryT[Q, M, A => B]): QueryT[Q, M, B] = {
    QueryT(M.ap(ma.run)(M.map(mf.run) { (qf: Q[A => B]) =>
      { (qa: Q[A]) => Q.ap(qa)(qf) }
    }))
  }
}

trait QueryTHoist[Q[+_]] extends Hoist[({ type λ[m[+_], α] = QueryT[Q, m, α] })#λ] { self =>
  implicit def Q: SwappableMonad[Q]

  def liftM[M[+_], A](ma: M[A])(implicit M: Monad[M]): QueryT[Q, M, A] = QueryT[Q, M, A](M.map(ma)(Q.point(_)))

  def hoist[M[+_]: Monad, N[+_]](f: M ~> N) = new (({ type λ[α] = QueryT[Q, M, α] })#λ ~> ({ type λ[α] = QueryT[Q, N, α] })#λ) {
    def apply[A](ma: QueryT[Q, M, A]): QueryT[Q, N, A] = QueryT(f(ma.run))
  }

  implicit def apply[M[+_]](implicit M0: Monad[M]): Monad[({ type λ[+α] = QueryT[Q, M, α] })#λ] = new QueryTMonad[Q, M] {
    def Q = self.Q
    def M = M0
  }
}
