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
package com.precog
package daze

import com.precog.yggdrasil._
import com.precog.analytics.Path

import scalaz.{Identity => _, _}
import scalaz.effect._
import scalaz.iteratee._

import Iteratee._

case class DatasetEnum[X, E, F[_]](enum: EnumeratorP[X, E, F], descriptor: Option[ProjectionDescriptor] = None) {
  def map[E2](f: E => E2): DatasetEnum[X, E2, F] = 
    DatasetEnum(enum map f)

  def foldLeft[A](a: A)(f: (A, E) => A): DatasetEnum[X, A, F] = null

  def flatMap[E2](f: E => DatasetEnum[X, E2, F]): DatasetEnum[X, E2, F] = 
    DatasetEnum(enum.flatMap(e => f(e).enum))
  
  def collect[E2](pf: PartialFunction[E, E2]): DatasetEnum[X, E2, F] = 
    DatasetEnum(enum collect pf)

  def uniq(implicit ord: Order[E]): DatasetEnum[X, E, F] = 
    DatasetEnum(enum.uniq)

  def zipWithIndex: DatasetEnum[X, (E, Long), F] = 
    DatasetEnum(enum.zipWithIndex)

  def :^[E2](enum2: DatasetEnum[X, E2, F]): DatasetEnum[X, (E, E2), F] = 
    DatasetEnum(enum :^ enum2.enum)

  def ^:[E2](enum2: DatasetEnum[X, E2, F]): DatasetEnum[X, (E2, E), F] = 
    DatasetEnum(enum ^: (enum2.enum))

  def join(enum2: DatasetEnum[X, E, F])(implicit order: Order[E], m: Monad[F]): DatasetEnum[X, (E, E), F] =
    DatasetEnum(enum join enum2.enum)

  def merge(enum2: DatasetEnum[X, E, F])(implicit order: Order[E], monad: Monad[F]): DatasetEnum[X, E, F] =
    DatasetEnum(enum merge enum2.enum)
}

trait DatasetEnumOps {
  def cogroup[X, F[_]](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, Either3[SEvent, (SEvent, SEvent), SEvent], F] = 
    DatasetEnum(cogroupE[X, SEvent, SEvent, F].apply(enum1.enum, enum2.enum))

  def crossLeft[X, F[_]: Monad](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F]): DatasetEnum[X, (SEvent, SEvent), F] = 
    enum1 :^ enum2

  def crossRight[X, F[_]: Monad](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F]): DatasetEnum[X, (SEvent, SEvent), F] = 
    enum1 ^: enum2

  def join[X, F[_]](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, (SEvent, SEvent), F] =
    enum1 join enum2

  def merge[X, F[_]](enum1: DatasetEnum[X, SEvent, F], enum2: DatasetEnum[X, SEvent, F])(implicit order: Order[SEvent], monad: Monad[F]): DatasetEnum[X, SEvent, F] =
    enum1 merge enum2

  def sort[X](enum: DatasetEnum[X, SEvent, IO])(implicit order: Order[SEvent]): DatasetEnum[X, SEvent, IO] 
  
  def empty[X, E, F[_]: Monad]: DatasetEnum[X, E, F] = DatasetEnum(
    new EnumeratorP[X, E, F] {
      def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, E, G] = {
        import MO._
        Monoid[EnumeratorT[X, E, G]].zero
      }
    }
  )

  def point[X, E, F[_]: Monad](value: E): DatasetEnum[X, E, F] = DatasetEnum(
    new EnumeratorP[X, E, F] {
      def apply[G[_]](implicit MO: G |>=| F): EnumeratorT[X, E, G] = {
        import MO._
        EnumeratorT.enumOne[X, E, G](value)
      }
    }
  )

  def liftM[X, E, F[_]: Monad](value: F[E]): DatasetEnum[X, E, F] = DatasetEnum(
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

  def map[X, E1, E2, F[_]: Monad](enum: DatasetEnum[X, E1, F])(f: E1 => E2): DatasetEnum[X, E2, F] = 
    enum.map(f)

  def flatMap[X, E1, E2, F[_]: Monad](enum: DatasetEnum[X, E1, F])(f: E1 => DatasetEnum[X, E2, F]): DatasetEnum[X, E2, F] = 
    enum.flatMap(f)
     
  def collect[X, E1, E2, F[_]: Monad](enum: DatasetEnum[X, E1, F])(pf: PartialFunction[E1, E2]): DatasetEnum[X, E2, F] = 
    enum.collect(pf)
}


// vim: set ts=4 sw=4 et:
