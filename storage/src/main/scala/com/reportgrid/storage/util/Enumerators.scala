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
package com.reportgrid.storage
package util

import java.io.File
import java.nio.ByteBuffer
import java.util.Comparator
import scala.collection.mutable.ArrayBuffer

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.bind._

import Iteratee._

trait Enumerators {

  def sort[X, E <: AnyRef, A](bufferSize: Int, workDir: File, cacheId: String)(unsorted: EnumeratorP[X, E, IO])(implicit order: Order[E], b: Bijection[E, ByteBuffer], cm: ClassManifest[E]): EnumeratorP[X, E, IO] = {
    new EnumeratorP[X, E, IO] {
      def apply[F[_[_], _]: MonadTrans, A]: StepT[X, E, ({type l[α] = F[IO,α]})#l, A] => IterateeT[X, E, ({type l[α] = F[IO,α]})#l, A] = {
        type FIO[A] = F[IO, A]
        implicit val FMonad: Monad[FIO] = MonadTrans[F].apply[IO]

        val buffer = new Array[E](bufferSize)

        def sortBuf(to: Int): F[IO, Unit] = MonadTrans[F].liftM(IO {
          java.util.Arrays.sort[E](buffer, 0, to, order.toJavaComparator)
        })

        def bufferInsert(i: Int, el: E): F[IO, Unit] = MonadTrans[F].liftM(IO {
          buffer(i) = el
        })

        def consume(i: Int, contf: Input[E] => IterateeT[X, E, FIO, A]): IterateeT[X, E, FIO, A] = {
          if (i < bufferSize) cont { (in: Input[E]) => 
            in.fold(
              el = el => iterateeT(FMonad.bind(bufferInsert(i, el)) { _ => consume(i + 1, contf).value }),
              empty = consume(i, contf),
              eof   = iterateeT(FMonad.bind(sortBuf(i - 1)) { _ => (cont(contf) >>== enumArray[X, E, FIO, A](buffer, 0, Some(i - 1))).value })
            )
          } else {
            consumeToDisk(contf)
          }
        }

        def consumeToDisk(contf: Input[E] => IterateeT[X, E, FIO, A]): IterateeT[X, E, FIO, A] = {
          sys.error("todo")
        }

        (_: StepT[X, E, FIO, A]) mapCont { contf => consume(0, contf) }
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
