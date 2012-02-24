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
  def flatMapTimeout: Duration
}

trait YggdrasilEnumOpsComponent extends YggConfigComponent with DatasetEnumOpsComponent {
  type YggConfig <: YggEnumOpsConfig

  trait Ops extends DatasetEnumOps {
    def flatMap[X, E1, E2, F[_]](d: DatasetEnum[X, E1, F])(f: E1 => DatasetEnum[X, E2, F])(implicit M: Monad[F], asyncContext: ExecutionContext): DatasetEnum[X, E2, F] = 
      DatasetEnum(d.fenum.map { e1e =>
        val result : EnumeratorP[X, Vector[E2], F] = e1e.flatMap { ve => 
          val epv : Vector[EnumeratorP[X, Vector[E2], F]] = ve.map(e => Await.result(f(e).fenum, yggConfig.flatMapTimeout))
          epv.reduce { (ep1 : EnumeratorP[X, Vector[E2], F], ep2: EnumeratorP[X, Vector[E2], F]) => EnumeratorP.enumeratorPMonoid[X, Vector[E2], F].append(ep1, ep2) }
        }
        result
      })

    def sort[X, E <: AnyRef](d: DatasetEnum[X, E, IO], memoAs: Option[(Int, MemoizationContext)])(implicit order: Order[E], cm: ClassManifest[E], fs: FileSerialization[Vector[E]], asyncContext: ExecutionContext): DatasetEnum[X, E, IO] = {
      memoAs.getOrElse((scala.util.Random.nextInt, MemoizationContext.Noop)) match {
        case (memoId, ctx) => ctx[X, Vector[E]](memoId) match {
          case Right(memoized) => DatasetEnum(Future(memoized))
          case Left(memoizer)  =>
            DatasetEnum(
              d.fenum map { unsorted =>
                new EnumeratorP[X, Vector[E], IO] {
                  def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, Vector[E], F] = {
                    import MO._
                    implicit val MI = Monad[({type λ[α] = IterateeT[X, Vector[E], F, α]})#λ]

                    val buffer = new Array[E](yggConfig.sortBufferSize)

                    // TODO: derek says "rethink this whole thing...with chunks" (Daniel agrees).  so, get on that kris
                    def bufferInsert(i: Int, el: Vector[E]): F[Unit] = MO.promote(IO { el.zipWithIndex foreach { case (e, i2) => buffer(i + i2) = e } }) 

                    def enumBuffer(to: Int) = new EnumeratorP[X, Vector[E], IO] {
                      def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, Vector[E], F] = {
                        import MO._

                        java.util.Arrays.sort(buffer, 0, to, order.toJavaComparator) // TODO: Get this working inside of EnumeratorT.perform again
                        //EnumeratorT.perform[X, Vector[E], F, Unit](MO.promote(IO { java.util.Arrays.sort(buffer, 0, to, order.toJavaComparator) })) |+|
                        enumOne[X, Vector[E], F](Vector(buffer.slice(0, to): _*))
                      }
                    }

                    new EnumeratorT[X, Vector[E], F] {
                      def apply[A] = {
                        def sortFile(i: Int) = new File(yggConfig.sortWorkDir, "sort" + memoId + "." + i)

                        def consume(i: Int, chunks: Vector[File], contf: Input[Vector[E]] => IterateeT[X, Vector[E], F, A]): IterateeT[X, Vector[E], F, A] = {
                          if (i < yggConfig.sortBufferSize) cont { (in: Input[Vector[E]]) => 
                            in.fold(
                              el    = el => iterateeT(bufferInsert(i, el) >> consume(i + el.length, chunks, contf).value),
                              empty = consume(i, chunks, contf),
                              eof   = // once we've been sent EOF, we sort the buffer then finally rebuild the iteratee we were 
                                      // originally provided and use that to consume the sorted buffer. We have to pass EOF to
                                      // restore the EOF that we received that triggered the original processing of the stream.
                                      cont(contf) &= 
                                      mergeAll[X, E, IO](chunks.map(fs.reader[X]) :+ enumBuffer(i): _*).apply[F] &= 
                                      EnumeratorT.perform[X, Vector[E], F, Unit](MO.promote(chunks.foldLeft(IO()) { (io, f) => io >> IO(f.delete) })) &= 
                                      enumEofT
                            )
                          } else {
                            MI.bind(fs.writer(sortFile(chunks.size)) &= enumBuffer(i).apply[F]) { file => 
                              consume(0, chunks :+ file, contf)
                            }
                          }
                        }

                        (s: StepT[X, Vector[E], F, A]) => memoizer.memoizing(s mapCont { contf => consume(0, Vector.empty[File], contf) &= unsorted[F] })
                      }
                    }
                  }
                }
              }
            )
        }
      }
    }
    
    def memoize[X, E](d: DatasetEnum[X, E, IO], memoId: Int, memoctx: MemoizationContext)(implicit fs: FileSerialization[Vector[E]], asyncContext: ExecutionContext): DatasetEnum[X, E, IO] = {
      memoctx[X, Vector[E]](memoId) match {
        case Right(enum) => DatasetEnum(Future(enum))
        case Left(memoizer) =>
          DatasetEnum(
            d.fenum map { unmemoized =>
              new EnumeratorP[X, Vector[E], IO] {
                def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, Vector[E], F] = {
                  import MO._
                  import MO.MG.bindSyntax._

                  new EnumeratorT[X, Vector[E], F] {
                    def apply[A] = {
                      (s: StepT[X, Vector[E], F, A]) => memoizer.memoizing(s.pointI) &= unmemoized[F]
                    }
                  }
                }
              }
            }
          )
      }
    }

    def group[X](d: DatasetEnum[X, SEvent, IO])(f: SEvent => Key)(implicit ord: Order[Key], sfs: FileSerialization[(Key, SEvent)], buffering: Buffering[Vector[SEvent]], asyncContext: ExecutionContext): 
    Future[EnumeratorP[X, (Key, DatasetEnum[X, SEvent, IO]), IO]] = {
      sys.error("todo")
      /*
      type LE = Vector[(Key, SEvent)]
      type Group = (Key, DatasetEnum[X, SEvent, IO])
      
      implicit val pairOrder = ord.contramap((_: (Key, SEvent))._1)


      def chunked(enum: EnumeratorP[X, Vector[(Key, SEvent)], IO]): EnumeratorP[X, Group, IO] = {
        new EnumeratorP[X, Group, IO] {
          def apply[G[_]](implicit MO: G |>=| IO) = new EnumeratorT[X, Group, G] {
            import MO._

            def apply[A] = {
              def outerLoop(last: Option[Key], buffer: Vector[SEvent], s: StepT[X, Group, G, A]): IterateeT[X, LE, G, StepT[X, Group, G, A]] = {
                // the inner loop will consume elements into the specified iteratee until the key
                // changes, then will run the outer loop.
                def loop(last: Option[Key], buffer: Vector[SEvent], bufStep: StepT[X, Vector[SEvent], G, EnumeratorP[X, Vector[SEvent], IO]], groupStep: StepT[X, Group, G, A]): Input[LE] => IterateeT[X, LE, G, StepT[X, Group, G, A]] = {
                  def loopDone(iter: IterateeT[X, Vector[SEvent], G, EnumeratorP[X, Vector[SEvent], IO]], remainder: Input[LE]): IterateeT[X, LE, G, StepT[X, Group, G, A]] = 
                    iter map { bufEnum => groupStep.mapCont(_(elInput((k, bufEnum)))) >>== outerLoop(last, buffer, _) }

                  (_: Input[LE]).fold(
                    el = el => 
                      el.headOption match {
                        case Some((k, _)) if last.forall(_ == k) => 
                          val (prefix, remainder) = el.span(_._1 == k)
                          val merged = buffer ++ prefix.map(_._2)

                          if (remainder.isEmpty) {
                            if (merged.size < yggConfig.sortBufferSize) {
                              // we don't know whether the next chunk will have more data corresponding to this key, so
                              // we just continue, appending to our buffer but not advancing the bufstep.
                              cont(loop[G](last, merged, bufStep))
                            } else {
                              // we need to advance the bufstep with a chunk of maximal size, then
                              // continue with the rest in the buffer.
                              val (chunk, rest) = merged.splitAt(yggConfig.sortBufferSize)
                              iterateeT(
                                for {
                                  bufStep  <- bufStep.mapCont(_(elInput(chunk))).value
                                  loopStep <- cont[X, LE, G, Result](loop[G](last, rest, bufStep)).value
                                } yield loopStep
                              )
                            }
                          } else {
                            if (merged.size < yggConfig.sortBufferSize) {
                              // we need to advance the bufstep just once, then be done.
                              loopDone(bufStep.mapCont(_(elInput(merged))), elInput(remainder))
                            } else {
                              //advance the bufstep with a chunk of maximal size, then the rest, then be done.
                              val (chunk, rest) = merged.splitAt(yggConfig.sortBufferSize)
                              loopDone(bufStep.mapCont(_(elInput(chunk))) >>== (s => s.mapCont(_(elInput(rest)))), elInput(remainder))
                            }
                          }

                        case Some(_) =>
                          loopDone(bufStep.pointI, elInput(el))
                          
                        case None =>
                          cont(loop[G](last, buffer, bufStep))
                      },
                    empty = cont(loop[G](last, buffer, bufStep)),
                    eof   = loopDone(bufStep.mapCont(_(elInput(buffer))), eofInput)
                  )
                }

                s.mapCont(k => cont(loop(last, buffer, buffering[X, G], cont(k))))
              }

              outerLoop(None, Vector(), _)
            }
          }
        }
      }

      d.fenum map { enum => 
        new EnumeratorP[X, Group, IO] {
          def apply[G[_]](implicit MO: G |>=| IO) = new EnumeratorT[X, Group, G] {
            import MO._

            def apply[A] = {
              val memoId = 0 //todo, fix this
              def sortFile(i: Int) = new File(yggConfig.sortWorkDir, "groupsort" + memoId + "." + i)

              val buffer = new Array[(Key, SEvent)](yggConfig.sortBufferSize)
              def bufferInsert(i: Int, el: Vector[(Key, SEvent)]): G[Unit] = MO.promote(IO { el.zipWithIndex foreach { case (e, i2) => buffer(i + i2) = e } }) 
              def enumBuffer(to: Int) = new EnumeratorP[X, Vector[(Key, SEvent)], IO] {
                def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, Vector[(Key, SEvent)], F] = {
                  import MO._

                  java.util.Arrays.sort(buffer, 0, to, pairOrder.toJavaComparator) // TODO: Get this working inside of EnumeratorT.perform again
                  //EnumeratorT.perform[X, Vector[SEvent], F, Unit](MO.promote(IO { java.util.Arrays.sort(buffer, 0, to, order.toJavaComparator) })) |+|
                  enumOne[X, Vector[(Key, SEvent)], F](Vector(buffer.slice(0, to): _*))
                }
              }

              def consume(i: Int, chunks: Vector[File]): IterateeT[X, Vector[SEvent], G, (Int, Vector[File])] = {
                if (i < yggConfig.sortBufferSize) cont { (in: Input[Vector[SEvent]]) => 
                  in.fold(
                    el    = el => iterateeT(bufferInsert(i, el map { ev => (f(ev), ev) }) >> consume(i + el.length, chunks).value),
                    empty = consume(i, chunks),
                    eof   = done((i, chunks), eofInput)
                  )
                } else {
                  fs.writer(sortFile(chunks.size)).withResult(enumBuffer(i).apply[G]) { file => 
                    consume(0, chunks :+ file)
                  }
                }
              }

              (s: StepT[X, Group, G, A]) => consume(0, Vector.empty[File]).withResult(enum[G]) {
                case (i, files) => chunked(mergeAll(files.map(fs.reader[X]) :+ enumBuffer(i): _*)).apply[G].apply(s)
              }
            }
          }
        }
      }
      */
    }
  }
}

// vim: set ts=4 sw=4 et:
