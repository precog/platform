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
  def memoizationSerialization: FileSerialization[Vector[SEvent]]
}

trait DiskMemoizationComponent extends YggConfigComponent with MemoizationComponent { 
  type YggConfig <: DiskMemoizationConfig

  def withMemoizationContext[A](f: MemoContext => A) = f(new MemoContext { })

  sealed trait MemoContext extends MemoizationContext { self => 
    val serialization = yggConfig.memoizationSerialization
    import serialization._

    @volatile private var cache = Map.empty[Int, (Option[ProjectionDescriptor], Either[Vector[Vector[SEvent]], File])]

    def apply[X](memoId: Int)(implicit asyncContext: ExecutionContext): Either[MemoizationContext.Memoizer[X], DatasetEnum[X, SEvent, IO]] = self.synchronized {
      cache.get(memoId) match {
        case Some((descriptor, Left(vector))) => 
          Right(DatasetEnum(Future(EnumeratorP.enumPStream[X, Vector[SEvent], IO](vector.toStream)), descriptor))
          
        case Some((descriptor, Right(file))) => 
          Right(DatasetEnum(Future(reader[X](file)), descriptor))

        case None => Left(
          new MemoizationContext.Memoizer[X] {
            def apply[F[_], A](d: Option[ProjectionDescriptor])(implicit MO: F |>=| IO) = (iter: IterateeT[X, Vector[SEvent], F, A]) => {
              import MO._
              (iter zip memoizer[X, F](memoId)) map {
                case (result, memo) => 
                  self.synchronized { if (!cache.isDefinedAt(memoId)) {
                    cache += (memoId -> (d, memo)) 
                  }}
                  result
              }
            }
          }
        )
      }
    }

    def expire(memoId: Int) = IO {
      self.synchronized { cache -= memoId }
    }

    def purge = IO {
      self.synchronized {
        cache collect { case (_, (_, Right(file))) => file.delete }
        cache = Map()
      }
    }

    def memoizer[X, F[_]](memoId: Int)(implicit MO: F |>=| IO): IterateeT[X, Vector[SEvent], F, Either[Vector[Vector[SEvent]], File]] = {
      import MO._

      def consume(i: Int, acc: Vector[Vector[SEvent]]): IterateeT[X, Vector[SEvent], F, Either[Vector[Vector[SEvent]], File]] = {
        if (i < yggConfig.memoizationBufferSize) 
          cont(
            (_: Input[Vector[SEvent]]).fold(
              el    = el => consume(i + 1, acc :+ el),
              empty = consume(i, acc),
              eof   = done(Left(acc), eofInput)))
        else 
          (writer(new File(yggConfig.memoizationWorkDir, "memo" + memoId)) &= enumStream[X, Vector[SEvent], F](acc.toStream)) map (f => Right(f))
      }

      consume(0, Vector.empty[Vector[SEvent]])
    }
  }
}
