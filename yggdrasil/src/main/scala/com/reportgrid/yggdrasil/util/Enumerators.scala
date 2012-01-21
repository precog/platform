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

import Iteratee._

trait Enumerators {

  def sort[X, E <: AnyRef, A](bufferSize: Int, workDir: File, projectionDescriptor: ProjectionDescriptor)(unsorted: EnumeratorP[X, E, IO])(implicit order: Order[E], b: Bijection[E, (Array[Byte], Array[Byte])], cm: ClassManifest[E]): EnumeratorP[X, E, IO] = {
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

        def buildLevelDBProjection = IO {
          //val projection = LevelDBProjection(workDir, projectionDescriptor)
        }

        def consumeToDisk(contf: Input[E] => IterateeT[X, E, FIO, A]): IterateeT[X, E, FIO, A] = {
          // build a new LevelDBProjection

          sys.error("Disk-based sorts not yet supported.")
        }

        (_: StepT[X, E, FIO, A]) mapCont { contf => consume(0, contf) }
      }
    }
  }
}

object Enumerators extends Enumerators

// vim: set ts=4 sw=4 et:
