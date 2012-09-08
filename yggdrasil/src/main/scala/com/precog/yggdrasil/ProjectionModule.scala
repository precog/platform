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
package com.precog.yggdrasil

import scalaz.Order
import scalaz.effect._

trait ProjectionModule {
  type Projection <: ProjectionLike

  val Projection: ProjectionCompanion

  trait ProjectionCompanion {
    def open(descriptor: ProjectionDescriptor): IO[Projection]

    def close(p: Projection): IO[Unit]

    def archive(p: Projection): IO[Boolean]
  }
}

trait ProjectionLike {
  def descriptor: ProjectionDescriptor

  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit]
}

case class BlockProjectionData[Key, Block](minKey: Key, maxKey: Key, data: Block)

trait BlockProjectionLike[Key, Block] extends ProjectionLike {
  //TODO: make the following type member work instead of having a type parameter
  // type Key

  //implicit def keyOrder: Order[Key]

  /** 
   * Get a block of data beginning with the first record with a key greater than
   * the specified key. If id.isEmpty, return a block starting with the minimum
   * key. Each resulting block should contain only the columns specified in the 
   * column set; if the set of columns is empty, return all columns.
   */
  def getBlockAfter(id: Option[Key], columns: Set[ColumnDescriptor] = Set()): Option[BlockProjectionData[Key, Block]]
}

trait FullProjectionLike[+Dataset] extends ProjectionLike {
  def allRecords(expiresAt: Long): Dataset
}
