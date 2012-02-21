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

import com.precog.yggdrasil._
import com.precog.yggdrasil.shard._
import com.precog.yggdrasil.util._
import com.precog.util._

import akka.dispatch.Await
import akka.dispatch.ExecutionContext
import akka.dispatch.Future
import akka.util.Duration
import java.io.File

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.monad._
import scalaz.syntax.semigroup._
import Iteratee._

trait YggEnumOpsConfig {
  def sortBufferSize: Int
  def sortWorkDir: File
  def sortSerialization: FileSerialization[SEvent]
  def flatMapTimeout: Duration
}

trait YggdrasilEnumOpsComponent extends YggConfigComponent with DatasetEnumOpsComponent {
  type YggConfig <: YggEnumOpsConfig

  trait Ops extends DatasetEnumOps {
    val serialization = yggConfig.sortSerialization
    import serialization._

    def flatMap[X, E1, E2, F[_]](d: DatasetEnum[X, E1, F])(f: E1 => DatasetEnum[X, E2, F])(implicit M: Monad[F], asyncContext: ExecutionContext): DatasetEnum[X, E2, F] = 
      DatasetEnum(d.fenum.map { e1e =>
        val result : EnumeratorP[X, Vector[E2], F] = e1e.flatMap { ve => 
          val epv : Vector[EnumeratorP[X, Vector[E2], F]] = ve.map(e => Await.result(f(e).fenum, yggConfig.flatMapTimeout))
          epv.reduce { (ep1 : EnumeratorP[X, Vector[E2], F], ep2: EnumeratorP[X, Vector[E2], F]) => EnumeratorP.enumeratorPMonoid[X, Vector[E2], F].append(ep1, ep2) }
        }
        result
      })

    def sort[X](d: DatasetEnum[X, SEvent, IO], memoAs: Option[(Int, MemoizationContext)])(implicit order: Order[SEvent], asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO] = {
      import MemoizationContext._
      memoAs.getOrElse((scala.util.Random.nextInt, MemoizationContext.Noop)) match {
        case (memoId, ctx) => ctx[X](memoId) match {
          case Right(memoized) => memoized
          case Left(memoizer)  =>
            DatasetEnum(
              d.fenum map { unsorted =>
                new EnumeratorP[X, Vector[SEvent], IO] {
                  def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, Vector[SEvent], F] = {
                    import MO._
                    implicit val MI = Monad[({type λ[α] = IterateeT[X, Vector[SEvent], F, α]})#λ]

                    val buffer = new Array[SEvent](yggConfig.sortBufferSize)
                    def bufferInsert(i: Int, el: Vector[SEvent]): F[Unit] = MO.promote(IO { el.zipWithIndex foreach { case (e, i2) => buffer(i + i2) = e } }) // TODO: derek says "rethink this whole thing...with chunks" (Daniel agrees).  so, get on that kris
                    def enumBuffer(to: Int) = new EnumeratorP[X, Vector[SEvent], IO] {
                      def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, Vector[SEvent], F] = {
                        import MO._

                        java.util.Arrays.sort(buffer, 0, to, order.toJavaComparator) // TODO: Get this working inside of EnumeratorT.perform again
                        //EnumeratorT.perform[X, Vector[SEvent], F, Unit](MO.promote(IO { java.util.Arrays.sort(buffer, 0, to, order.toJavaComparator) })) |+|
                        enumOne[X, Vector[SEvent], F](Vector(buffer.slice(0, to): _*))
                      }
                    }

                    new EnumeratorT[X, Vector[SEvent], F] {
                      def apply[A] = {
                        val memof = memoizer[F, A](d.descriptor)
                        def sortFile(i: Int) = new File(yggConfig.sortWorkDir, "sort" + memoId + "." + i)

                        def consume(i: Int, chunks: Vector[File], contf: Input[Vector[SEvent]] => IterateeT[X, Vector[SEvent], F, A]): IterateeT[X, Vector[SEvent], F, A] = {
                          if (i < yggConfig.sortBufferSize) cont { (in: Input[Vector[SEvent]]) => 
                            in.fold(
                              el    = el => iterateeT(bufferInsert(i, el) >> consume(i + el.length, chunks, contf).value),
                              empty = consume(i, chunks, contf),
                              eof   = // once we've been sent EOF, we sort the buffer then finally rebuild the iteratee we were 
                                      // originally provided and use that to consume the sorted buffer. We have to pass EOF to
                                      // restore the EOF that we received that triggered the original processing of the stream.
                                      cont(contf) &= 
                                      mergeAll(chunks.map(reader[X]) :+ enumBuffer(i): _*).apply[F] &= 
                                      EnumeratorT.perform[X, Vector[SEvent], F, Unit](MO.promote(chunks.foldLeft(IO()) { (io, f) => io >> IO(f.delete) })) &= 
                                      enumEofT
                            )
                          } else {
                            MI.bind(writer(sortFile(chunks.size)) &= enumBuffer(i).apply[F]) { file => 
                              consume(0, chunks :+ file, contf)
                            }
                          }
                        }

                        (s: StepT[X, Vector[SEvent], F, A]) => memof(s mapCont { contf => consume(0, Vector.empty[File], contf) &= unsorted[F] })
                      }
                    }
                  }
                }
              }
            )
        }
      }
    }
    
    def memoize[X](d: DatasetEnum[X, SEvent, IO], memoId: Int, memoizationContext: MemoizationContext)(implicit asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO] = {
      memoizationContext[X](memoId) match {
        case Right(enum) => enum
        case Left(memoizer) =>
          DatasetEnum(
            d.fenum map { unmemoized =>
              new EnumeratorP[X, Vector[SEvent], IO] {
                def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, Vector[SEvent], F] = {
                  import MO._
                  import MO.MG.bindSyntax._

                  new EnumeratorT[X, Vector[SEvent], F] {
                    def apply[A] = {
                      val memof = memoizer[F, A](d.descriptor)
                      (s: StepT[X, Vector[SEvent], F, A]) => memof(s.pointI) &= unmemoized[F]
                    }
                  }
                }
              }
            }
          )
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
