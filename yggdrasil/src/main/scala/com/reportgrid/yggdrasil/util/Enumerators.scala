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
