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
package com.precog.bifrost

import org.joda.time.DateTime

import blueeyes.util.Close

import scalaz._

case class QueryResource[A](a: A, close0: Close[A]) {
  def close(): Unit = close0.close(a)
}

sealed trait JobQueryState[+A] {
  import JobQueryState._

  def getOrElse[AA >: A](aa: => AA) = this match {
    case Cancelled | Expired => aa
    case Running(_, value) => value
  }
}

object JobQueryState {
  case object Cancelled extends JobQueryState[Nothing] { def value = None }
  case object Expired extends JobQueryState[Nothing] { def value = None }
  case class Running[A](resources: Set[QueryResource[_]], value0: A) extends JobQueryState[A] {
    def value = Some(value0)
  }
}

trait JobQueryStateMonad extends SwappableMonad[JobQueryState] {
  import JobQueryState._

  /** Returns `true` if the job has been cancelled. */
  def isCancelled(): Boolean

  /** Returns `true` if the job has expired. */
  def hasExpired(): Boolean

  /** Force the job to immediately abort. */
  def abort(): Boolean

  def swap[M[+_], A](state: JobQueryState[M[A]])(implicit M: Monad[M]): M[JobQueryState[A]] = {
    state match {
      case Running(resources, ma) => M.map(ma)(Running(resources, _))
      case Cancelled => M.point(Cancelled)
      case Expired => M.point(Expired)
    }
  }

  def point[A](a: => A): JobQueryState[A] = if (isCancelled()) {
    Cancelled
  } else if (hasExpired()) {
    Expired
  } else {
    Running(Set.empty, a)
  }

  def maybeCancel[A](q: JobQueryState[A]): JobQueryState[A] = if (isCancelled()) {
    // Free resources from q.
    Cancelled
  } else if (hasExpired()) {
    Expired
  } else {
    q
  }

  override def map[A, B](fa: JobQueryState[A])(f: A => B): JobQueryState[B] = maybeCancel(fa) match {
    case Running(resources, value) => Running(resources, f(value))
    case Cancelled => Cancelled
    case Expired => Expired
  }

  def bind[A, B](fa: JobQueryState[A])(f: A => JobQueryState[B]): JobQueryState[B] = maybeCancel(fa) match {
    case Running(resources0, value0) => f(value0) match {
      case Running(resources1, value) => Running(resources0 ++ resources1, value)
      case Cancelled => Cancelled
      case Expired => Expired
    }
    case Cancelled => Cancelled
    case Expired => Expired
  }
}
