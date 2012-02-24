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
import com.precog.util.KMap

import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import java.io._
import java.util.zip._

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.monad._
import Iteratee._

trait DiskMemoizationConfig {
  def memoizationBufferSize: Int
  def memoizationWorkDir: File
}

trait DiskMemoizationComponent extends YggConfigComponent with MemoizationComponent { self =>
  type YggConfig <: DiskMemoizationConfig

  def withMemoizationContext[A](f: MemoContext => A) = f(new MemoContext { })

  sealed trait MemoContext extends MemoizationContext { ctx => 
    type CacheKey[α] = Int
    type CacheValue[α] = Either[Vector[α], File]
    @volatile private var cache: KMap[CacheKey, CacheValue] = KMap.empty[CacheKey, CacheValue]
    @volatile private var files: List[File] = Nil

    def apply[X, E](memoId: Int)(implicit fs: FileSerialization[E], asyncContext: ExecutionContext): Either[Memoizer[X, E], EnumeratorP[X, E, IO]] = ctx.synchronized {
      cache.get(memoId) match {
        case Some(Left(vector)) => 
          Right(EnumeratorP.enumPStream[X, E, IO](vector.toStream))
          
        case Some(Right(file)) => 
          Right(fs.reader[X](file))

        case None => Left(
          new Memoizer[X, E] {
            def memoizing[F[_], A](iter: IterateeT[X, E, F, A])(implicit MO: F |>=| IO) = {
              import MO._
              (iter zip memoizer[X, E, F](memoId)) map {
                case (result, memo) => 
                  ctx.synchronized { if (!cache.isDefinedAt(memoId)) {
                    for (f <- memo.right) files = f :: files
                    cache += (memoId -> memo) 
                  }}
                  result
              }
            }
          }
        )
      }
    }

    def expire(memoId: Int) = IO {
      ctx.synchronized { cache -= memoId }
    }

    def purge = IO {
      ctx.synchronized {
        for (f <- files) f.delete
        files = Nil
        cache = KMap.empty[CacheKey, CacheValue]
      }
    }

    def memoizer[X, E, F[_]](memoId: Int)(implicit fs: FileSerialization[E], MO: F |>=| IO): IterateeT[X, E, F, Either[Vector[E], File]] = {
      import MO._

      def consume(i: Int, acc: Vector[E]): IterateeT[X, E, F, Either[Vector[E], File]] = {
        if (i < yggConfig.memoizationBufferSize) 
          cont(
            (_: Input[E]).fold(
              el    = el => consume(i + 1, acc :+ el),
              empty = consume(i, acc),
              eof   = done(Left(acc), eofInput)))
        else 
          (fs.writer(new File(yggConfig.memoizationWorkDir, "memo" + memoId)) &= enumStream[X, E, F](acc.toStream)) map (f => Right(f))
      }

      consume(0, Vector.empty[E])
    }
  }
}
