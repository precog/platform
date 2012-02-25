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

    def sort[X, E <: AnyRef](d: DatasetEnum[X, E, IO], memoAs: Option[(Int, MemoizationContext)])(implicit order: Order[E], cm: Manifest[E], fs: FileSerialization[Vector[E]], asyncContext: ExecutionContext): DatasetEnum[X, E, IO] = {
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
                    val insert = (i: Int, e: E) => { buffer(i) = e; i + 1 }

                    def bufferInsert(i: Int, el: Vector[E]): F[Unit] = {
                      MO promote { IO { el.foldLeft(i) { insert } } }
                    }

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
                                      mergeAllChunked[X, E, IO](chunks.map(fs.reader[X]) :+ enumBuffer(i): _*).apply[F] &= 
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
    
    def memoize[X, E](d: DatasetEnum[X, E, IO], memoId: Int, memoctx: MemoizationContext)(implicit fs: FileSerialization[Vector[E]], asyncContext: ExecutionContext): DatasetEnum[X, E, IO] = 
      DatasetEnum(
        d.fenum map { unmemoized =>
          new EnumeratorP[X, Vector[E], IO] {
            def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, Vector[E], F] = {
              import MO._
              import MO.MG.bindSyntax._

              new EnumeratorT[X, Vector[E], F] {
                def apply[A] = (s: StepT[X, Vector[E], F, A]) => 
                  memoctx[X, Vector[E]](memoId) match {
                    case Right(enum) => s.pointI &= enum[F]
                    case Left(memoizer) => memoizer.memoizing(s.pointI) &= unmemoized[F]
                  }
              }
            }
          }
        }
      )

    def group[X](d: DatasetEnum[X, SEvent, IO])(f: SEvent => Key)(implicit ord: Order[Key], fs: FileSerialization[Vector[(Key, SEvent)]], buffering: Buffering[Vector[SEvent]], asyncContext: ExecutionContext): 
    Future[EnumeratorP[X, (Key, DatasetEnum[X, SEvent, IO]), IO]] = {
      type LE = Vector[(Key, SEvent)]
      type Group = (Key, DatasetEnum[X, SEvent, IO])
      
      implicit val pairOrder = ord.contramap((_: (Key, SEvent))._1)

      def chunked[G[_]](implicit MO: G |>=| IO): EnumerateeT[X, Vector[(Key, SEvent)], Group, G] = new EnumerateeT[X, Vector[(Key, SEvent)], Group, G] {
        import MO._

        def apply[A]: StepT[X, Group, G, A] => IterateeT[X, Vector[(Key, SEvent)], G, StepT[X, Group, G, A]] = step => {
          step.fold(
            cont = (contf: Input[Group] => IterateeT[X, Group, G, A]) =>
              headDoneOr[X, Vector[(Key, SEvent)], G, StepT[X, Group, G, A]](
                scont(contf),
                v => v.headOption match {
                  case Some((key, _)) => iterateeT(buffering[X, G].value.map(s => scont(loop(key, Vector(), s)))).flatMap(g => contf(elInput(g)) >>== apply[A])
                  case None           => contf(emptyInput) >>== apply[A]
                }
              ),
            done = (a, r) => done(sdone(a, r), emptyInput),
            err  = x => err(x)
          )
        }

        def loop(last: Key, buffer: Vector[SEvent], bufStep: StepT[X, Vector[SEvent], G, EnumeratorP[X, Vector[SEvent], IO]]): Input[Vector[(Key, SEvent)]] => IterateeT[X, Vector[(Key, SEvent)], G, Group] = {
          def loopDone(bufIter: IterateeT[X, Vector[SEvent], G, EnumeratorP[X, Vector[SEvent], IO]], remainder: Input[Vector[(Key, SEvent)]]) = 
            iterateeT(
              (bufIter &= enumEofT).foldT(
                cont = contf  => sys.error("diverging iteratee"),
                done = (a, _) => MO.MG.point(sdone[X, Vector[(Key, SEvent)], G, Group]((last, DatasetEnum(Future(a))), remainder)),
                err  = x      => MO.MG.point(serr[X, Vector[(Key, SEvent)], G, Group](x))
              )
            )
            
          (in: Input[Vector[(Key, SEvent)]]) => {
            in.fold(
              el = el => 
                el.headOption match {
                  case Some((`last`, _)) =>
                    val (prefix, remainder) = el.span(_._1 == last)
                    val merged = buffer ++ prefix.map(_._2)

                    if (remainder.isEmpty) {
                      if (merged.size < yggConfig.sortBufferSize) {
                        // we don't know whether the next chunk will have more data corresponding to this key, so
                        // we just continue, appending to our buffer but not advancing the bufstep.
                        cont(loop(last, merged, bufStep))
                      } else {
                        // we need to advance the bufstep with a chunk of maximal size, then
                        // continue with the rest in the buffer.
                        val (chunk, rest) = merged.splitAt(yggConfig.sortBufferSize)
                        iterateeT(
                          for {
                            bufStep  <- bufStep.mapCont(_(elInput(chunk))).value
                            loopStep <- cont(loop(last, rest, bufStep)).value
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
                        loopDone((bufStep.mapCont(_(elInput(chunk))) >>== (s => s.mapCont(_(elInput(rest))))), elInput(remainder))
                      }
                    }

                  case Some(_) => 
                    loopDone(bufStep.pointI, in)

                  case None =>
                    cont(loop(last, buffer, bufStep))
                },
              empty = cont(loop(last, buffer, bufStep)),
              eof   = loopDone(bufStep.mapCont(_(elInput(buffer))), eofInput)
            )
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
              def bufferInsert(i: Int, v: Vector[(Key, SEvent)]): G[Unit] = {
                val insert = (i: Int, e: (Key, SEvent)) => { buffer(i) = e; i + 1 }
                MO promote { IO { v.foldLeft(i) { insert } } }
              }

              def enumBuffer(to: Int) = new EnumeratorP[X, LE, IO] {
                def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, LE, F] = {
                  import MO._

                  EnumeratorT.perform[X, LE, F, Unit](MO.promote(IO { java.util.Arrays.sort(buffer, 0, to, pairOrder.toJavaComparator) })) |+|
                  enumOne[X, LE, F](Vector(buffer.slice(0, to): _*))
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
                case (i, files) => 
                  val chunks: Seq[EnumeratorP[X, Vector[(Key, SEvent)], IO]] = files.map(fs.reader[X]) :+ enumBuffer(i)
                  s.pointI &= chunked[G].run(mergeAllChunked(chunks: _*).apply[G])
              }
            }
          }
        }
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
