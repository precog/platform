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

import scalaz.effect._
import scalaz.iteratee._
import java.nio.ByteBuffer

trait Column {
  def getAllIds[F[_] : MonadIO, A] : EnumeratorT[Unit, Long, F, A]
  def getIdsInRange[F[_] : MonadIO, A](range : Interval[Long]): EnumeratorT[Unit, Long, F, A]
  def getIdsForValue[F[_] : MonadIO, A](v : ByteBuffer): EnumeratorT[Unit, Long, F, A]
  def getIdsByValueRange[F[_] : MonadIO, A](range : Interval[ByteBuffer]): EnumeratorT[Unit, Long, F, A]

  def getAllValues[F[_] : MonadIO, A] : EnumeratorT[Unit, ByteBuffer, F, A]
  def getValuesInRange[F[_]: MonadIO, A](range: Interval[ByteBuffer]): EnumeratorT[Unit, ByteBuffer, F, A]
  def getValueForId[F[_]: MonadIO, A](id: Long): EnumeratorT[Unit, ByteBuffer, F, A]
  def getValuesByIdRange[F[_]: MonadIO, A](range: Interval[Long]): EnumeratorT[Unit, ByteBuffer, F, A]
}

