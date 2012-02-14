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

trait YggdrasilEnumOpsComponent extends YggConfigComponent with DatasetEnumOpsComponent with MemoizationComponent {
  type YggConfig <: YggEnumOpsConfig

  trait Ops extends DatasetEnumOps {
    def flatMap[X, E1, E2, F[_]](d: DatasetEnum[X, E1, F])(f: E1 => DatasetEnum[X, E2, F])(implicit M: Monad[F], asyncContext: ExecutionContext): DatasetEnum[X, E2, F] = 
      DatasetEnum(d.fenum.map(_.flatMap(e => Await.result(f(e).fenum, yggConfig.flatMapTimeout))))

    def sort[X](d: DatasetEnum[X, SEvent, IO], memoId: Option[Int])(implicit order: Order[SEvent], asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO] = {
      val memoContext = memoizationContext[X]
      memoId.toRight(memoContext.noopMemoizer).right.flatMap(memoContext.apply) match {
        case Right(enum) => enum
        case Left(memoizer) =>
          DatasetEnum(
            d.fenum map { unsorted =>
              new EnumeratorP[X, SEvent, IO] {
                def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, SEvent, F] = {
                  import MO._
                  import MO.MG.bindSyntax._

                  new EnumeratorT[X, SEvent, F] {
                    val buffer = new Array[SEvent](yggConfig.sortBufferSize)

                    def sortBuf(to: Int): IO[Unit] = IO {
                      java.util.Arrays.sort(buffer, 0, to, order.toJavaComparator)
                    }

                    def bufferInsert(i: Int, el: SEvent): IO[Unit] = IO {
                      buffer(i) = el
                    }

                    def apply[A] = {
                      val memof = memoizer[F, A](d.descriptor)
                      def consume(i: Int, contf: Input[SEvent] => IterateeT[X, SEvent, F, A]): IterateeT[X, SEvent, F, A] = {
                        if (i < yggConfig.sortBufferSize) cont { (in: Input[SEvent]) => 
                          in.fold(
                            el    = el => iterateeT(MO.promote(bufferInsert(i, el)) >>= { _ => consume(i + 1, contf).value }),
                            empty = consume(i, contf),
                            eof   = 
                              // once we've been sent EOF, we sort the buffer then finally rebuild the iteratee we were 
                              // originally provided and use that to consume the sorted buffer. We have to pass EOF to
                              // restore the EOF that we received that triggered the original processing of the stream.
                              iterateeT(MO.promote(sortBuf(i)) >>= { _ => (cont(contf) &= enumArray[X, SEvent, F](buffer, 0, Some(i)) &= enumEofT).value })
                          )
                        } else {
                          consumeToDisk(contf)
                        }
                      }

                      def consumeToDisk(contf: Input[SEvent] => IterateeT[X, SEvent, F, A]): IterateeT[X, SEvent, F, A] = {
                        // build a new LevelDBProjection
                        sys.error("Disk-based sorts not yet supported.")
                      }

                      (s: StepT[X, SEvent, F, A]) => memof(s mapCont { contf => consume(0, contf) &= unsorted[F] })
                    }
                  }
                }
              }
            }
          )
      }
    }
    
    def memoize[X](d: DatasetEnum[X, SEvent, IO], memoId: Int)(implicit asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO] = {
      val memoContext = memoizationContext[X]
      memoContext(memoId) match {
        case Right(enum) => enum
        case Left(memoizer) =>
          DatasetEnum(
            d.fenum map { unmemoized =>
              new EnumeratorP[X, SEvent, IO] {
                def apply[F[_]](implicit MO: F |>=| IO): EnumeratorT[X, SEvent, F] = {
                  import MO._
                  import MO.MG.bindSyntax._

                  new EnumeratorT[X, SEvent, F] {
                    def apply[A] = {
                      val memof = memoizer[F, A](d.descriptor)
                      (s: StepT[X, SEvent, F, A]) => memof(s.pointI) &= unmemoized[F]
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
