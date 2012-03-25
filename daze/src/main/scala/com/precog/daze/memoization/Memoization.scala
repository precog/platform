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
package memoization

import yggdrasil._
import yggdrasil.serialization._

import akka.dispatch.ExecutionContext

import java.io._
import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import IterateeT._


trait MemoCache {
  def expire(memoId: MemoId): IO[Unit]
  def purge: IO[Unit]
}

object MemoCache {
  object Noop extends MemoCache {
    def expire(memoId: Int) = IO(())
    def purge = IO(())
  }
}

trait MemoizationContext {
  def cache: MemoCache
}

trait IteratorMemoizationContext extends MemoizationContext {
  type Dataset[E]
  def memoizing[E](memoId: Int): Either[Dataset[E] => Dataset[E], Dataset[E]]
}

trait IterateeMemoizationContext extends MemoizationContext {
  trait Memoizer[X, E] {
    def apply[F[_], A](iter: IterateeT[X, E, F, A])(implicit MO: F |>=| IO): IterateeT[X, E, F, A]
  }

  def memoizing[X, E](memoId: Int)(implicit fs: IterateeFileSerialization[E], asyncContext: ExecutionContext): Either[Memoizer[X, E], EnumeratorP[X, E, IO]]
}

object IterateeMemoizationContext {
  trait Noop extends IterateeMemoizationContext {
    def memoizing[X, E](memoId: Int)(implicit fs: IterateeFileSerialization[E], asyncContext: ExecutionContext): Either[Memoizer[X, E], EnumeratorP[X, E, IO]] = Left(
      new Memoizer[X, E] {
        def apply[F[_], A](iter: IterateeT[X, E, F, A])(implicit MO: F |>=| IO) = iter
      }
    )
  }

  object Noop extends Noop {
    val cache = MemoCache.Noop
  }
}

trait BufferingContext {
  def cache: MemoCache

  def buffering[X, E, F[_]](memoId: Int)(implicit fs: IterateeFileSerialization[E], MO: F |>=| IO): IterateeT[X, E, F, EnumeratorP[X, E, IO]]
}

object BufferingContext {
  trait Memory extends BufferingContext {
    def bufferSize: Int
    def buffering[X, E, F[_]](memoId: Int)(implicit fs: IterateeFileSerialization[E], MO: F |>=| IO): IterateeT[X, E, F, EnumeratorP[X, E, IO]] = {
      import MO._
      import scalaz.std.list._
      take[X, E, F, List](bufferSize).map(l => EnumeratorP.enumPStream[X, E, IO](l.toStream))
    }
  }

  def memory(size: Int) = new Memory {
    val cache = MemoCache.Noop
    val bufferSize = size
  }
}

trait MemoEnvironment {
  type MemoContext <: MemoizationContext

  def withMemoizationContext[A](f: MemoContext => A): A
}

trait BufferingEnvironment {
  type MemoContext

  def withMemoizationContext[A](f: MemoContext => A): A
}

// vim: set ts=4 sw=4 et:
