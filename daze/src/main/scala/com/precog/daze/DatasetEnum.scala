package com.precog
package daze

import akka.dispatch.ExecutionContext
import akka.dispatch.Future

import com.precog.yggdrasil._

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.monoid._
import scalaz.syntax.monad._
import scalaz.syntax.arrow._
import scalaz.std.function._
import scalaz.std.partialFunction._

import Iteratee._

case class DatasetEnum[E, F[_]](fenum: Future[EnumeratorP[Unit, Vector[(Identities, E)], F]], descriptor: Option[ProjectionDescriptor] = None) {
  /*
  type Chunk[T] = Vector[(Identities, T)]

  def map[E2](f: E => E2): DatasetEnum[X, E2, F] = 
    DatasetEnum(fenum map (_ map (_ map f.second)))

  def reduce[E2](b: Option[E2])(f: (Option[E2], E) => Option[E2]): DatasetEnum[X, E2, F] = DatasetEnum(
    fenum map { enum => 
      new EnumeratorP[X, Chunk[E2], F] {
        def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, Chunk[E2], G] = {
          import MO._
          new EnumeratorT[X, Chunk[E2], G] {
            def apply[A] = (step: StepT[X, Chunk[E2], G, A]) => {
              def check(s: StepT[X, Chunk[E], G, Option[E2]]): IterateeT[X, Chunk[E2], G, A] = s.fold(
                cont = k => k(eofInput) >>== { 
                  s => s.mapContOr(_ => sys.error("diverging iteratee"), check(s))
                }
                , done = (opt, _) => opt.map(v => step.mapCont(cf => cf(elInput(Vector((Identities.Empty, v)))))).getOrElse(step.pointI)
                , err  = x => err(x)
              )

              val chunkFold: (Option[E2], Chunk[E]) => Option[E2] = {
                case (init, chunk) => chunk.foldLeft(init) {
                  case (acc, (_, v)) => f(acc, v)
                }
              }

              iterateeT((IterateeT.fold[X, Chunk[E], G, Option[E2]](b)(chunkFold) &= enum[G]).value >>= (s => check(s).value))
            }
          }
        }
      }
    }
  )

  def collect[E2](pf: PartialFunction[E, E2]): DatasetEnum[X, E2, F] = 
    DatasetEnum(fenum map (_ map (_ collect pf.second)))

  def uniq(implicit ord: Order[E]): DatasetEnum[X, E, F] = 
    DatasetEnum(fenum map (_.uniqChunk))

  def zipWithIndex: DatasetEnum[X, (E, Long), F] = 
    DatasetEnum(fenum map (_.zipChunkWithIndex))

  def crossLeft[E2](d2: DatasetEnum[X, E2, F]): DatasetEnum[X, (E, E2), F] = 
    DatasetEnum(fenum flatMap (enum => d2.fenum map (enum :^ _)))

  def crossRight[E2](d2: DatasetEnum[X, E2, F]): DatasetEnum[X, (E, E2), F] = 
    DatasetEnum(fenum flatMap { enum => d2.fenum map { _.:^[E2, E, Vector[E]](enum) map { _ map { case (a, b) => (b, a) } } } })

  def join(d2: DatasetEnum[X, E, F])(implicit order: Order[E], m: Monad[F]): DatasetEnum[X, (E, E), F] =
    DatasetEnum(fenum flatMap (enum => d2.fenum map (enum.joinChunked[E])))

  def merge(d2: DatasetEnum[X, E, F])(implicit order: Order[E], monad: Monad[F]): DatasetEnum[X, E, F] =
    DatasetEnum(fenum flatMap (enum => d2.fenum map (enum.mergeChunked[E])))

  def perform[B](f: F[B])(implicit m: Monad[F]): DatasetEnum[X, E, F] = 
    DatasetEnum(fenum map { enum => EnumeratorP.enumeratorPMonoid[X, Vector[E], F].append(enum, EnumeratorP.perform[X, Vector[E], F, B](f)) }, descriptor)

  */
}

trait DatasetEnumOps {
  /*
  def cogroup[X, F[_]](d1: DatasetEnum[X, SEvent, F], d2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, Either3[SEvent, (SEvent, SEvent), SEvent], F] = 
    DatasetEnum(for (en1 <- d1.fenum; en2 <- d2.fenum) yield cogroupEChunked[X, SEvent, SEvent, F](monad, order.order _, order, order).apply(en1, en2))

  def crossLeft[X, F[_]: Monad](d1: DatasetEnum[X, SEvent, F], d2: DatasetEnum[X, SEvent, F]): DatasetEnum[X, (SEvent, SEvent), F] = 
    d1 crossLeft d2

  def crossRight[X, F[_]: Monad](d1: DatasetEnum[X, SEvent, F], d2: DatasetEnum[X, SEvent, F]): DatasetEnum[X, (SEvent, SEvent), F] = 
    d1 crossRight d2

  def join[X, F[_]](d1: DatasetEnum[X, SEvent, F], d2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, (SEvent, SEvent), F] =
    d1 join d2

  def merge[X, F[_]](d1: DatasetEnum[X, SEvent, F], d2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, SEvent, F] =
    d1 merge d2

  def map[X, E1, E2, F[_]: Monad](d: DatasetEnum[X, E1, F])(f: E1 => E2): DatasetEnum[X, E2, F] = 
    d.map(f)

  def collect[X, E1, E2, F[_]: Monad](d: DatasetEnum[X, E1, F])(pf: PartialFunction[E1, E2]): DatasetEnum[X, E2, F] = 
    d.collect(pf)

  def empty[X, E, F[_]](implicit M: Monad[F], asyncContext: ExecutionContext): DatasetEnum[X, E, F] = DatasetEnum(
    Future(
      new EnumeratorP[X, Vector[E], F] {
        def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, Vector[E], G] = {
          import MO._
          Monoid[EnumeratorT[X, Vector[E], G]].zero
        }
      }
    )
  )

  def point[X, E, F[_]](value: Vector[E])(implicit M: Monad[F], asyncContext: ExecutionContext): DatasetEnum[X, E, F] = DatasetEnum(
    Future(
      new EnumeratorP[X, Vector[E], F] {
        def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, Vector[E], G] = {
          import MO._
          EnumeratorT.enumOne[X, Vector[E], G](value)
        }
      }
    )
  )

  def liftM[X, E, F[_]](value: F[Vector[E]])(implicit M: Monad[F], asyncContext: ExecutionContext): DatasetEnum[X, E, F] = DatasetEnum(
    Future(
      new EnumeratorP[X, Vector[E], F] {
        def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, Vector[E], G] = new EnumeratorT[X, Vector[E], G] {
          import MO._
          import MO.MG.bindSyntax._

          def apply[A] = { (step: StepT[X, Vector[E], G, A]) => 
            iterateeT[X, Vector[E], G, A](MO.promote(value) >>= { e => step.mapCont(f => f(elInput(e))).value })
          }
        }
      }
    )
  )

  type X = Throwable

  def flatMap[E1, E2, F[_]](d: DatasetEnum[X, E1, F])(f: E1 => DatasetEnum[X, E2, F])(implicit M: Monad[F], asyncContext: ExecutionContext): DatasetEnum[X, E2, F] 

  def sort[E <: AnyRef](d: DatasetEnum[X, E, IO], memoAs: Option[(Int, MemoizationContext)])(implicit order: Order[E], cm: Manifest[E], fs: FileSerialization[Vector[E]], asyncContext: ExecutionContext): DatasetEnum[X, E, IO] 
  
  def memoize[E](d: DatasetEnum[X, E, IO], memoId: Int, memoctx: MemoizationContext)(implicit fs: FileSerialization[Vector[E]], asyncContext: ExecutionContext): DatasetEnum[X, E, IO] 

  // result must be (stably) ordered by key!!!!
  type Key = List[SValue]
  def group(d: DatasetEnum[X, SEvent, IO], memoId: Int, bufctx: BufferingContext)(keyFor: SEvent => Key)
           (implicit ord: Order[Key], fs: FileSerialization[Vector[SEvent]], kvs: FileSerialization[Vector[(Key, SEvent)]], asyncContext: ExecutionContext): 
           Future[EnumeratorP[X, (Key, DatasetEnum[X, SEvent, IO]), IO]] 

  */
}

// vim: set ts=4 sw=4 et:
