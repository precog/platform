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
package com.precog.daze
package memoization

import com.precog.yggdrasil._
import com.precog.yggdrasil.serialization._
import com.precog.util._

import akka.dispatch.Future
import akka.dispatch.Promise
import akka.dispatch.ExecutionContext
import java.io._
import java.util.zip._

import scala.annotation.tailrec
import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.monad._
import scalaz.syntax.semigroup._
import scalaz.syntax.std._
import Iteratee._

trait DiskMemoizationConfig {
  def memoizationBufferSize: Int
  def memoizationWorkDir: File
}

trait DiskIterableDatasetMemoizationComponent extends YggConfigComponent with MemoizationEnvironment { self =>
  type YggConfig <: DiskMemoizationConfig
  type Dataset[α] = IterableDataset[α]

  implicit val asyncContext: ExecutionContext

  def withMemoizationContext[A](f: MemoContext => A) = f(new MemoContext { })

  sealed trait MemoContext extends MemoizationContext[IterableDataset] { ctx => 
    type CacheKey[α] = Int
    type CacheValue[α] = Promise[(Int, Either[Vector[α], File])]
    @volatile private var memoCache: KMap[CacheKey, CacheValue] = KMap.empty[CacheKey, CacheValue]
    @volatile private var files: List[File] = Nil

    def memoizing[A](memoId: Int)(implicit serialization: IncrementalSerialization[(Identities, A)]): Either[IterableDataset[A] => Future[IterableDataset[A]], Future[IterableDataset[A]]] = {
      type IA = (Identities, A)
      def store(iter: Iterator[IA]): Either[Vector[IA], File] = {
        @tailrec def buffer(i: Int, acc: Vector[IA]): Vector[IA] = {
          if (i < yggConfig.memoizationBufferSize && iter.hasNext) buffer(i+1, acc :+ iter.next)
          else acc
        }

        @tailrec def spill(writer: serialization.IncrementalWriter, out: DataOutputStream, iter: Iterator[IA]): serialization.IncrementalWriter = {
          if (iter.hasNext) spill(writer.write(out, iter.next), out, iter)
          else writer
        }

        val initial = buffer(0, Vector()) 
        if (initial.length < yggConfig.memoizationBufferSize) {
          Left(initial)
        } else {
          val memoFile = new File(yggConfig.memoizationWorkDir, "memo." + memoId)
          using(serialization.oStream(memoFile)) { out =>
            spill(spill(serialization.writer, out, initial.iterator), out, iter).finish(out)
          }

          Right(memoFile)
        }
      }

      def datasetFor(future: Future[(Int, Either[Vector[IA], File])]) = future map {
        case (idCount, Left(vector)) =>
          IterableDataset(idCount, vector)

        case (idCount, Right(file)) => 
          IterableDataset(idCount, new Iterable[IA] { def iterator = serialization.reader.read(serialization.iStream(file), true) })
      }

      ctx.synchronized {
        memoCache.get(memoId) match {
          case Some(future) => Right(datasetFor(future))

          case None => 
            val promise = Promise[(Int, Either[Vector[IA], File])]
            memoCache += (memoId -> promise)
            Left((toMemoize: IterableDataset[A]) => {
              datasetFor(promise.completeWith(Future((toMemoize.idCount, store(toMemoize.iterable.iterator)))))
            })
        }
      }
    }

    object cache extends MemoCache {
      def expire(memoId: Int) = IO {
        ctx.synchronized { memoCache -= memoId }
      }

      def purge = IO {
        ctx.synchronized {
          for (f <- files) f.delete
          files = Nil
          memoCache = KMap.empty[CacheKey, CacheValue]
        }
      }
    }
  }
}

/*
trait DiskIterateeMemoizationComponent extends YggConfigComponent with MemoizationEnvironment { self =>
  type YggConfig <: DiskMemoizationConfig

  def withMemoizationContext[A](f: MemoContext => A) = f(new MemoContext { })

  sealed trait MemoContext extends IterateeMemoizationContext with BufferingContext { ctx => 
    type CacheKey[α] = Int
    type CacheValue[α] = Either[Vector[α], File]
    @volatile private var memoCache: KMap[CacheKey, CacheValue] = KMap.empty[CacheKey, CacheValue]
    @volatile private var files: List[File] = Nil

    private def memoizer[X, E, F[_]](memoId: Int)(implicit fs: IterateeFileSerialization[E], MO: F |>=| IO): IterateeT[X, E, F, Either[Vector[E], File]] = {
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

    def buffering[X, E, F[_]](memoId: Int)(implicit fs: IterateeFileSerialization[E], MO: F |>=| IO): IterateeT[X, E, F, EnumeratorP[X, E, IO]] = ctx.synchronized {
      import MO._
      memoCache.get(memoId) match {
        case Some(Left(vector)) => 
          done[X, E, F, EnumeratorP[X, E, IO]](EnumeratorP.enumPStream[X, E, IO](vector.toStream), eofInput)
          
        case Some(Right(file)) => 
          done[X, E, F, EnumeratorP[X, E, IO]](fs.reader[X](file), eofInput)

        case None =>
          import MO._
          memoizer[X, E, F](memoId) map { memo =>
            EnumeratorP.perform[X, E, IO, Unit](IO(ctx.synchronized { if (!memoCache.isDefinedAt(memoId)) memoCache += (memoId -> memo) })) |+|
            memo.fold(
              vector => EnumeratorP.enumPStream[X, E, IO](vector.toStream),
              file   => fs.reader[X](file)
            )
          }
      }
    }

    def memoizing[X, E](memoId: Int)(implicit fs: IterateeFileSerialization[E], asyncContext: ExecutionContext): Either[Memoizer[X, E], EnumeratorP[X, E, IO]] = ctx.synchronized {
      memoCache.get(memoId) match {
        case Some(Left(vector)) => 
          Right(EnumeratorP.enumPStream[X, E, IO](vector.toStream))
          
        case Some(Right(file)) => 
          Right(fs.reader[X](file))

        case None => Left(
          new Memoizer[X, E] {
            def apply[F[_], A](iter: IterateeT[X, E, F, A])(implicit MO: F |>=| IO) = {
              import MO._
              (iter zip memoizer[X, E, F](memoId)) map {
                case (result, memo) => 
                  ctx.synchronized { if (!memoCache.isDefinedAt(memoId)) {
                    for (f <- memo.right) files = f :: files
                    memoCache += (memoId -> memo) 
                  }}
                  result
              }
            }
          }
        )
      }
    }

    object cache extends MemoCache {
      def expire(memoId: Int) = IO {
        ctx.synchronized { memoCache -= memoId }
      }

      def purge = IO {
        ctx.synchronized {
          for (f <- files) f.delete
          files = Nil
          memoCache = KMap.empty[CacheKey, CacheValue]
        }
      }
    }
  }
}

*/
