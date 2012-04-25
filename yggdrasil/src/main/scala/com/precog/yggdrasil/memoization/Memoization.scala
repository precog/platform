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
package com.precog.yggdrasil
package memoization

import serialization._

trait MemoizationEnvironment {
  type Memoable[E]
  type MemoContext <: MemoizationContext[Memoable]

  def withMemoizationContext[A](f: MemoContext => A): A
}

trait MemoCache {
  def expire(memoId: MemoId): Unit
  def purge(): Unit
}

object MemoCache {
  object Noop extends MemoCache {
    def expire(memoId: Int) = ()
    def purge() = ()
  }
}

trait MemoizationContext[Memoable[_]] {
  def cache: MemoCache
  def memoize[A](dataset: Memoable[A], memoId: Int)(implicit serialization: IncrementalSerialization[A]): Memoable[A]
  def sort[A](values: Memoable[A], memoId: Int)(implicit buffering: Buffering[A], fs: SortSerialization[A]): Memoable[A]
}
