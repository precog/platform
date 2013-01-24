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

import com.precog.util.PrecogUnit

import scalaz.Order
import scalaz.effect._

trait ProjectionModule[M[+_], Key, Block] {
  type Projection <: ProjectionLike[M, Key, Block]
  type ProjectionCompanion <: ProjectionCompanionLike

  def Projection: ProjectionCompanion

  // TODO: Should these really live here, or run through MetadataActor or ProjectionsActor directly?
  trait ProjectionCompanionLike {
    def apply(descriptor: ProjectionDescriptor): M[Projection]
  }
}

case class BlockProjectionData[Key, Block](minKey: Key, maxKey: Key, data: Block)

trait ProjectionLike[M[+_], Key, Block] {
  def descriptor: ProjectionDescriptor

  /** 
   * Get a block of data beginning with the first record with a key greater than
   * the specified key. If id.isEmpty, return a block starting with the minimum
   * key. Each resulting block should contain only the columns specified in the 
   * column set; if the set of columns is empty, return all columns.
   */
  def getBlockAfter(id: Option[Key], columns: Set[ColumnDescriptor] = Set()): M[Option[BlockProjectionData[Key, Block]]]
}

trait RawProjectionModule[M[+_], Key, Block] extends ProjectionModule[M, Key, Block] {
  type Projection <: RawProjectionLike[M, Key, Block]
  type ProjectionCompanion <: RawProjectionCompanionLike

  trait RawProjectionCompanionLike extends ProjectionCompanionLike {
    def close(p: Projection): M[PrecogUnit]

    def archive(d: ProjectionDescriptor): M[Boolean]
  }
}

trait RawProjectionLike[M[+_], Key, Block] extends ProjectionLike[M, Key, Block] {
  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): M[PrecogUnit]

  def commit: M[PrecogUnit]
}

trait SortProjectionLike[M[+_], Key, Block] extends RawProjectionLike[M, Key, Block] {
  def descriptor = sys.error("Sort projections do not have full ProjectionDescriptors")
  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false) = sys.error("Insertion on sort projections is unsupported")
  def commit = sys.error("Commit on sort projections is unsupported")
}
