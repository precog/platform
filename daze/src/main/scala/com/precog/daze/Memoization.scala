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
package com.precog
package daze

import yggdrasil._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import java.io.File

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import IterateeT._

trait MemoizationContext[X] {
  trait Memoizer[X] {
    def apply[F[_], A](d: Option[ProjectionDescriptor])(implicit MO: F |>=| IO): IterateeT[X, SEvent, F, A] => IterateeT[X, SEvent, F, A]
  }

  def apply(memoId: Int)(implicit asyncContext: ExecutionContext): Either[Memoizer[X], DatasetEnum[X, SEvent, IO]]

  val noopMemoizer: Memoizer[X] = new Memoizer[X] {
    def apply[F[_], A](d: Option[ProjectionDescriptor])(implicit MO: F |>=| IO) = iter => iter
  }
}

object MemoizationContext {
  def Noop[X]: MemoizationContext[X] = new MemoizationContext[X] {
    def apply(memoId: Int)(implicit asyncContext: ExecutionContext) = Left(noopMemoizer)
  }
}

trait MemoizationComponent {
  type MemoContext[X] <: MemoizationContext[X]

  def memoizationContext[X]: MemoContext[X]
}

// vim: set ts=4 sw=4 et:
