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
package com.reportgrid.daze

import java.nio.ByteBuffer
import scalaz._

// QualifiedSelector(path: String, sel: JPath, valueType: ValueType)

trait EventTypes {
  type RawEvent = (Seq[Long], ByteBuffer)
}

trait StorageEngineInsertAPI {
}

//trait StorageEngineQueryAPI extends EventTypes {
//  def column(path: String, selector: JPath, valueType: ValueType): DatasetEnum[X, RawEvent, IO]
//
//  def columnRange(interval: Interval[ByteBuffer])(path: String, selector: JPath, valueType: ValueType): DatasetEnum[X, (Seq[Long], ByteBuffer), IO]
//}
//
//trait StorageEngineAPI extends StorageEngineInsertAPI with StorageEngineQueryAPI
//
//// ProjectionDescriptor describes PHYSICAL FORMAT of values in the byte buffer
//case class ValuesDescriptor(values: Seq[(ValueFormat, Set[Metadata])])
//case class DatasetEnum[X, E, F[_]](enum: EnumeratorP[X, E, F], identityDesc: Seq[Set[ColumnDescriptor]], valuesDesc: ValuesDescriptor)
//
//trait OperationsAPI extends EventTypes {
//  def cogroup[X, F[_]: Monad](enum1: DatasetEnum[X, RawEvent, F], enum2: DatasetEnum[X, RawEvent, F]): DatasetEnum[X, Either3[RawEvent, RawEvent, RawEvent], F]
//
//  def crossLeft[X, F[_]: Monad](enum1: DatasetEnum[X, RawEvent, F], enum2: DatasetEnum[X, RawEvent, F]): DatasetEnum[X, RawEvent, F]
//
//  def crossRight[X, F[_]: Monad](enum1: DatasetEnum[X, RawEvent, F], enum2: DatasetEnum[X, RawEvent, F]): DatasetEnum[X, RawEvent, F]
//
//  def matched[X, F[_]: Monad](enum1: DatasetEnum[X, RawEvent, F], enum2: DatasetEnum[X, RawEvent, F]): DatasetEnum[X, RawEvent, F]
//
//  def merge[X, F[_]: Monad](enum1: DatasetEnum[X, RawEvent, F], enum2: DatasetEnum[X, RawEvent, F]): DatasetEnum[X, Either[RawEvent, RawEvent], F]
//
//  def sortIdentity[X, F[_]: Monad](enum: DatasetEnum[X, RawEvent, F], identityIndex: Int): DatasetEnum[X, RawEvent, F]
//
//  def mapValues[X, F[_]: Monad](enum: DatasetEnum[X, RawEvent, F], d: (ValueDescriptor, ByteBuffer => ByteBuffer)): DatasetEnum[X, RawEvent, F]
//
//  def mapIdentities[X, F[_]: Monad](enum: DatasetEnum[X, RawEvent, F], d: (Seq[Set[ColumnDescriptor]], Seq[Long] => Seq[Long])): DatasetEnum[X, RawEvent, F]
//}

  
  


// vim: set ts=4 sw=4 et:
