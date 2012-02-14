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

trait MemoizationContext {
  def apply[X](memoId: Int)(implicit asyncContext: ExecutionContext): Either[MemoizationContext.Memoizer[X], DatasetEnum[X, SEvent, IO]]
}

object MemoizationContext {
  trait Memoizer[X] {
    def apply[F[_], A](d: Option[ProjectionDescriptor])(implicit MO: F |>=| IO): IterateeT[X, SEvent, F, A] => IterateeT[X, SEvent, F, A]
  }

  object Memoizer {
    def noop[X]: Memoizer[X] = new Memoizer[X] {
      def apply[F[_], A](d: Option[ProjectionDescriptor])(implicit MO: F |>=| IO) = iter => iter
    }
  }

  trait Noop extends MemoizationContext {
    def apply[X](memoId: Int)(implicit asyncContext: ExecutionContext) = Left(Memoizer.noop[X])
  }

  object Noop extends Noop
}

trait MemoizationComponent {
  type MemoContext <: MemoizationContext

  def memoizationContext: MemoContext
}

// vim: set ts=4 sw=4 et:
