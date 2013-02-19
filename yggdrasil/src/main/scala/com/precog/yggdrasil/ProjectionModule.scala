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

import com.precog.common._
import com.precog.util.PrecogUnit

import blueeyes.json.JValue

import scalaz._
import scalaz.syntax.monad._

trait ProjectionModule[M[+_], Key, Block] {
  type Projection <: ProjectionLike[M, Key, Block]
  type ProjectionCompanion <: ProjectionCompanionLike[M]

  def Projection: ProjectionCompanion

  trait ProjectionCompanionLike[M0[+_]] { self =>
    def apply(path: Path): M0[Option[Projection]]

    def liftM[T[_[+_], +_]](implicit T: Hoist[T], M0: Monad[M0]) = new ProjectionCompanionLike[({ type λ[+α] = T[M0, α] })#λ] {
      def apply(path: Path) = self.apply(path).liftM[T]
    }
  }
}


case class BlockProjectionData[Key, Block](minKey: Key, maxKey: Key, data: Block)

trait ProjectionLike[M[+_], Key, Block] {
  def structure: M[Set[ColumnRef]]
  def length: Long

  /** 
   * Get a block of data beginning with the first record with a key greater than
   * the specified key. If id.isEmpty, return a block starting with the minimum
   * key. Each resulting block should contain only the columns specified in the 
   * column set; if the set of columns is empty, return all columns.
   */
  def getBlockAfter(id: Option[Key], columns: Option[Set[ColumnRef]] = None)(implicit M: Monad[M]): M[Option[BlockProjectionData[Key, Block]]]
}

//trait RawProjectionModule[M[+_], Key, Block] extends ProjectionModule[M, Key, Block] { 
//  type Projection <: RawProjectionLike[M, Key, Block]
//  type ProjectionCompanion <: RawProjectionCompanionLike[M]
//
//  trait RawProjectionCompanionLike[M0[+_]] extends ProjectionCompanionLike[M0] { self =>
//    def close(p: Projection): M0[PrecogUnit]
//    def archive(d: ProjectionDescriptor): M0[Boolean]
//
//    override def liftM[T[_[+_], +_]](implicit T: Hoist[T], M0: Monad[M0]) = new RawProjectionCompanionLike[({ type λ[+α] = T[M0, α] })#λ] {
//      def apply(descriptor: ProjectionDescriptor) = self(descriptor).liftM[T]
//      def apply(path: Path) = self.apply(path).liftM[T]
//      def close(p: Projection) = self.close(p).liftM[T]
//      def archive(d: ProjectionDescriptor) = self.archive(d).liftM[T]
//    }
//  }
//}

trait RawProjectionLike[M[+_], Key, Block] extends ProjectionLike[M, Key, Block] {
  // def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): M[PrecogUnit]
  def insert(id : Identities, v : Seq[JValue]): M[PrecogUnit]
  def commit: M[PrecogUnit]
}

trait SortProjectionLike[M[+_], Key, Block] extends RawProjectionLike[M, Key, Block] {
  def descriptor = sys.error("Sort projections do not have full ProjectionDescriptors")
  def insert(id : Identities, v : Seq[JValue]) = sys.error("Insertion on sort projections is unsupported")
  def commit = sys.error("Commit on sort projections is unsupported")
}
