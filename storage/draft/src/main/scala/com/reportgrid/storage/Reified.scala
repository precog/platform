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
package com.reportgrid.storage

trait Dataset[I, A]

sealed trait Reified[A]

object Reified {
  class OptionT[A: Reified] extends Reified[Option[A]] {
    def reified = implicitly[Reified[A]]
  }
  object OptionT {
    def apply[A: Reified] = new OptionT[A]
  }
  implicit def ToReifiedOption[A: Reified] = OptionT[A]
  implicit object BooleanT extends Reified[Boolean]
  implicit object LongT extends Reified[Long]
  class Tuple2T[A: Reified, B: Reified] extends Reified[(A, B)] {
    def reified1 = implicitly[Reified[A]]

    def reified2 = implicitly[Reified[B]]
  }
  object Tuple2T {
    def apply[A: Reified, B: Reified] = new Tuple2T[A, B]
  }
  implicit def ToReifiedTuple2[A: Reified, B: Reified] = Tuple2T[A, B]
  class DatasetT[I: Reified, A: Reified] extends Reified[Dataset[I, A]] {
    def reified1 = implicitly[Reified[I]]

    def reified2 = implicitly[Reified[A]]
  }
  object DatasetT {
    def apply[I: Reified, A: Reified] = new DatasetT[I, A]
  }
  implicit def ToReifiedDataset[I: Reified, A: Reified] = DatasetT[I, A]
}