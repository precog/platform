package com.precog
package daze

import akka.dispatch.Await
import akka.dispatch.ExecutionContext
import akka.dispatch.Future

import com.precog.yggdrasil._
import com.precog.analytics.Path

import scalaz.{Identity => _, _}
import scalaz.effect._
import scalaz.iteratee._
import scalaz.syntax.monoid._
import scalaz.syntax.monad._

import Iteratee._

case class DatasetEnum[X, E, F[_]](fenum: Future[EnumeratorP[X, E, F]], descriptor: Option[ProjectionDescriptor] = None) {
  def map[E2](f: E => E2): DatasetEnum[X, E2, F] = 
    DatasetEnum(fenum map (_ map f))

  def reduce[E2](b: Option[E2])(f: (Option[E2], E) => Option[E2]): DatasetEnum[X, E2, F] = DatasetEnum(
    fenum map { enum => 
      new EnumeratorP[X, E2, F] {
        def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, E2, G] = {
          import MO._
          new EnumeratorT[X, E2, G] {
            def apply[A] = (step: StepT[X, E2, G, A]) => {
              def check(s: StepT[X, E, G, Option[E2]]): IterateeT[X, E2, G, A] = s.fold(
                cont = k => k(eofInput) >>== { 
                  s => s.mapContOr(_ => sys.error("diverging iteratee"), check(s))
                }
                , done = (opt, _) => opt.map(v => step.mapCont(f => f(elInput(v)))).getOrElse(step.pointI)
                , err  = x => err(x)
              )

              iterateeT((IterateeT.fold[X, E, G, Option[E2]](b)(f) &= enum[G]).value >>= (s => check(s).value))
            }
          }
        }
      }
    }
  )

  def collect[E2](pf: PartialFunction[E, E2]): DatasetEnum[X, E2, F] = 
    DatasetEnum(fenum map (_ collect pf))

  def uniq(implicit ord: Order[E]): DatasetEnum[X, E, F] = 
    DatasetEnum(fenum map (_.uniq))

  def zipWithIndex: DatasetEnum[X, (E, Long), F] = 
    DatasetEnum(fenum map (_.zipWithIndex))

  def :^[E2](d2: DatasetEnum[X, E2, F]): DatasetEnum[X, (E, E2), F] = 
    DatasetEnum(fenum flatMap (enum => d2.fenum map (enum :^ _)))

  def ^:[E2](d2: DatasetEnum[X, E2, F]): DatasetEnum[X, (E, E2), F] = 
    DatasetEnum(fenum flatMap (enum => d2.fenum map (enum :^ _)))

  def join(d2: DatasetEnum[X, E, F])(implicit order: Order[E], m: Monad[F]): DatasetEnum[X, (E, E), F] =
    DatasetEnum(fenum flatMap (enum => d2.fenum map (enum join)))

  def merge(d2: DatasetEnum[X, E, F])(implicit order: Order[E], monad: Monad[F]): DatasetEnum[X, E, F] =
    DatasetEnum(fenum flatMap (enum => d2.fenum map (enum merge)))

  def perform[B](f: F[B])(implicit m: Monad[F]): DatasetEnum[X, E, F] = 
    DatasetEnum(fenum map { enum => EnumeratorP.enumeratorPMonoid[X, E, F].append(enum, EnumeratorP.perform[X, E, F, B](f)) }, descriptor)
}

trait DatasetEnumOps {
  def cogroup[X, F[_]](d1: DatasetEnum[X, SEvent, F], d2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, Either3[SEvent, (SEvent, SEvent), SEvent], F] = 
    DatasetEnum(for (en1 <- d1.fenum; en2 <- d2.fenum) yield cogroupE[X, SEvent, SEvent, F](monad, order.order _).apply(en1, en2))

  def crossLeft[X, F[_]: Monad](d1: DatasetEnum[X, SEvent, F], d2: DatasetEnum[X, SEvent, F]): DatasetEnum[X, (SEvent, SEvent), F] = 
    d1 :^ d2

  def crossRight[X, F[_]: Monad](d1: DatasetEnum[X, SEvent, F], d2: DatasetEnum[X, SEvent, F]): DatasetEnum[X, (SEvent, SEvent), F] = 
    d1 ^: d2

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
      new EnumeratorP[X, E, F] {
        def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, E, G] = {
          import MO._
          Monoid[EnumeratorT[X, E, G]].zero
        }
      }
    )
  )

  def point[X, E, F[_]](value: E)(implicit M: Monad[F], asyncContext: ExecutionContext): DatasetEnum[X, E, F] = DatasetEnum(
    Future(
      new EnumeratorP[X, E, F] {
        def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, E, G] = {
          import MO._
          EnumeratorT.enumOne[X, E, G](value)
        }
      }
    )
  )

  def liftM[X, E, F[_]](value: F[E])(implicit M: Monad[F], asyncContext: ExecutionContext): DatasetEnum[X, E, F] = DatasetEnum(
    Future(
      new EnumeratorP[X, E, F] {
        def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, E, G] = new EnumeratorT[X, E, G] {
          import MO._
          import MO.MG.bindSyntax._

          def apply[A] = { (step: StepT[X, E, G, A]) => 
            iterateeT[X, E, G, A](MO.promote(value) >>= { e => step.mapCont(f => f(elInput(e))).value })
          }
        }
      }
    )
  )

  def flatMap[X, E1, E2, F[_]](d: DatasetEnum[X, E1, F])(f: E1 => DatasetEnum[X, E2, F])(implicit M: Monad[F], asyncContext: ExecutionContext): DatasetEnum[X, E2, F] 

  def sort[X](d: DatasetEnum[X, SEvent, IO], memoId: Option[Int])(implicit order: Order[SEvent], asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO]
  
  def memoize[X](d: DatasetEnum[X, SEvent, IO], memoId: Int)(implicit asyncContext: ExecutionContext): DatasetEnum[X, SEvent, IO]
}

// vim: set ts=4 sw=4 et:
