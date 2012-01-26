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
package com.reportgrid.yggdrasil
package util

import com.reportgrid.util._
import java.io.File
import java.nio.ByteBuffer
import java.util.Comparator
import scala.collection.mutable.ArrayBuffer

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.bind._
import scalaz.std.stream._

import Iteratee._

trait Enumerators {
  def sort[X](unsorted: EnumeratorP[X, SEvent, IO], bufferSize: Int, workDir: File, projectionDescriptor: Option[ProjectionDescriptor])(implicit order: Order[SEvent]): EnumeratorP[X, SEvent, IO] = {

    new EnumeratorP[X, SEvent, IO] {
      def apply[F[_[_], _]: MonadTrans]: EnumeratorT[X, SEvent, ({type l[α] = F[IO,α]})#l] = {
        type FIO[α] = F[IO, α]
        implicit val FMonad: Monad[FIO] = MonadTrans[F].apply[IO]

        new EnumeratorT[X, SEvent, FIO] {
          val buffer = new Array[SEvent](bufferSize)

          def sortBuf(to: Int): F[IO, Unit] = MonadTrans[F].liftM(IO {
            java.util.Arrays.sort(buffer, 0, to, order.toJavaComparator)
          })

          def bufferInsert(i: Int, el: SEvent): F[IO, Unit] = MonadTrans[F].liftM(IO {
            buffer(i) = el
          })

          def apply[A] = {
            def consume(i: Int, contf: Input[SEvent] => IterateeT[X, SEvent, FIO, A]): IterateeT[X, SEvent, FIO, A] = {
              if (i < bufferSize) cont { (in: Input[SEvent]) => 
                in.fold(
                  el    = el => iterateeT(FMonad.bind(bufferInsert(i, el)) { _ => consume(i + 1, contf).value }),
                  empty = consume(i, contf),
                  eof   = 
                    // once we've been sent EOF, we sort the buffer then finally rebuild the iteratee we were 
                    // originally provided and use that to consume the sorted buffer. We have to pass EOF to
                    // restore the EOF that we received that triggered the original processing of the stream.
                    iterateeT(FMonad.bind(sortBuf(i)) { _ => (cont(contf) &= enumArray[X, SEvent, FIO](buffer, 0, Some(i)) &= enumEofT).value })
                )
              } else {
                consumeToDisk(contf)
              }
            }

            def consumeToDisk(contf: Input[SEvent] => IterateeT[X, SEvent, FIO, A]): IterateeT[X, SEvent, FIO, A] = {
              // build a new LevelDBProjection
              sys.error("Disk-based sorts not yet supported.")
            }

            (_: StepT[X, SEvent, FIO, A]) mapCont { contf => consume(0, contf) &= unsorted[F] }
          }
        }
      }
    }
  }
}

object Enumerators extends Enumerators

// vim: set ts=4 sw=4 et:
