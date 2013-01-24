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
