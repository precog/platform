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

    def sort[X](d: DatasetEnum[X, SEvent, IO], memoAs: Option[(Int, MemoizationContext)])(implicit order: Order[SEvent], asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO] = {
      import MemoizationContext._
      memoAs.toRight(Memoizer.noop[X]).right.flatMap({ case (i, ctx) => ctx[X](i) }) match {
        case Right(enum) => enum
        case Left(memoizer) =>
          DatasetEnum(
            d.fenum map { unsorted =>
              new EnumeratorP[X, Vector[SEvent], IO] {
                def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, Vector[SEvent], F] = {
                  import MO._
                  import MO.MG.bindSyntax._

                  new EnumeratorT[X, Vector[SEvent], F] {
                    val buffer = new Array[SEvent](yggConfig.sortBufferSize)

                    def sortBuf(to: Int): IO[Unit] = IO {
                      java.util.Arrays.sort(buffer, 0, to, order.toJavaComparator)
                    }

                    def bufferInsert(i: Int, el: Vector[SEvent]): IO[Unit] = IO {
                      el.zipWithIndex foreach { case (e, i2) => buffer(i + i2) = e }      // TODO: derek says "rethink this whole thing...with chunks" (Daniel agrees).  so, get on that kris
                    }

                    def apply[A] = {
                      val memof = memoizer[F, A](d.descriptor)
                      def consume(i: Int, contf: Input[Vector[SEvent]] => IterateeT[X, Vector[SEvent], F, A]): IterateeT[X, Vector[SEvent], F, A] = {
                        if (i < yggConfig.sortBufferSize) cont { (in: Input[Vector[SEvent]]) => 
                          in.fold(
                            el    = el => iterateeT(MO.promote(bufferInsert(i, el)) >>= { _ => consume(i + el.length, contf).value }),
                            empty = consume(i, contf),
                            eof   = 
                              // once we've been sent EOF, we sort the buffer then finally rebuild the iteratee we were 
                              // originally provided and use that to consume the sorted buffer. We have to pass EOF to
                              // restore the EOF that we received that triggered the original processing of the stream.
                              iterateeT(MO.promote(sortBuf(i)) >>= { _ => (cont(contf) &= enumOne[X, Vector[SEvent], F](Vector(buffer.slice(0, i): _*)) &= enumEofT).value })
                          )
                        } else {
                          consumeToDisk(contf)
                        }
                      }

                      def consumeToDisk(contf: Input[Vector[SEvent]] => IterateeT[X, Vector[SEvent], F, A]): IterateeT[X, Vector[SEvent], F, A] = {
                        // build a new LevelDBProjection
                        sys.error("Disk-based sorts not yet supported.")
                      }

                      (s: StepT[X, Vector[SEvent], F, A]) => memof(s mapCont { contf => consume(0, contf) &= unsorted[F] })
                    }
                  }
                }
              }
            }
          )
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
