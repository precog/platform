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
package com.querio.daze

case class IdentitySource(sources: Set[ProjectionDescriptor])
case class EventMatcher(order: Order[DEvent], merge: (Vector[Identity], Vector[Identity]) => Vector[Identity])

case class DatasetEnum[X, E, F[_]](enum: EnumeratorP[X, E, F], identityDerivation: Vector[IdentitySource])

// QualifiedSelector(path: String, sel: JPath, valueType: EType)
trait StorageEngineInsertAPI

trait StorageEngineQueryAPI {
  def column(path: Path): DatasetEnum[X, DEvent, IO]

  //def column(path: String, selector: JPath, valueType: EType): DatasetEnum[X, DEvent, IO]
  //def columnRange(interval: Interval[ByteBuffer])(path: String, selector: JPath, valueType: EType): DatasetEnum[X, (Seq[Long], ByteBuffer), IO]
}

trait StorageEngineAPI extends StorageEngineInsertAPI with StorageEngineQueryAPI 


trait OperationsAPI {
  def cogroup[X, F[_]: Monad](enum1: DatasetEnum[X, DEvent, F], enum2: DatasetEnum[X, DEvent, F])(implicit matcher: EventMatcher): DatasetEnum[X, Either3[DEvent, (DEvent, DEvent), DEvent], F]

  def crossLeft[X, F[_]: Monad](enum1: DatasetEnum[X, DEvent, F], enum2: DatasetEnum[X, DEvent, F]): DatasetEnum[X, DEvent, F]

  def crossRight[X, F[_]: Monad](enum1: DatasetEnum[X, DEvent, F], enum2: DatasetEnum[X, DEvent, F]): DatasetEnum[X, DEvent, F]

  def join[X, F[_]: Monad](enum1: DatasetEnum[X, DEvent, F], enum2: DatasetEnum[X, DEvent, F])(implicit matcher: EventMatcher): DatasetEnum[X, (DEvent, DEvent), F]

  def merge[X, F[_]: Monad](enum1: DatasetEnum[X, DEvent, F], enum2: DatasetEnum[X, DEvent, F])(implicit matcher: EventMatcher): DatasetEnum[X, DEvent, F]

  def sort[X, F[_]: Monad](enum: DatasetEnum[X, DEvent, F], identityIndex: Int): DatasetEnum[X, DEvent, F]

  def vmap[X, F[_]: Monad](enum: DatasetEnum[X, DEvent, F])(f: SValue => SValue): DatasetEnum[X, DEvent, F]
  def pmap[X, F[_]: Monad](enum: DatasetEnum[X, (DEvent, DEvent), F])(f: (SValue, SValue) => SValue): DatasetEnum[X, DEvent, F]

  def filter[X, F[_]: Monad](enum: DatasetEnum[X, (DEvent, DEvent), F])(pred: (SValue, SValue) => Option[SValue]): DatasetEnum[X, DEvent, F]

  def lift[X, F[_]: Monad](value: SValue): DatasetEnum[X, DEvent, F]
}
