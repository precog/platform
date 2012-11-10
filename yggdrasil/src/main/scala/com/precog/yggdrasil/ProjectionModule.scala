package com.precog.yggdrasil

import com.precog.util.PrecogUnit

import scalaz.Order
import scalaz.effect._

trait ProjectionModule {
  type Projection <: ProjectionLike

  val Projection: ProjectionCompanion

  // TODO: Should these really live here, or run through MetadataActor or ProjectionsActor directly?
  trait ProjectionCompanion {
    def open(descriptor: ProjectionDescriptor): IO[Projection]

    def close(p: Projection): IO[PrecogUnit]

    def archive(d: ProjectionDescriptor): IO[Boolean]
  }
}

trait ProjectionLike {
  def descriptor: ProjectionDescriptor

  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): Unit

  def commit(): IO[PrecogUnit]
}

trait SortProjectionLike extends ProjectionLike {
  def descriptor: ProjectionDescriptor = sys.error("Sort projections do not have full ProjectionDescriptors")
  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): Unit = sys.error("Insertion on sort projections is unsupported")
  def commit(): IO[PrecogUnit] = sys.error("Commit on sort projections is unsupported")
}

case class BlockProjectionData[Key, Block](minKey: Key, maxKey: Key, data: Block)

trait BlockProjectionLike[Key, Block] extends ProjectionLike {
  //TODO: make the following type member work instead of having a type parameter
  // type Key

  /** 
   * Get a block of data beginning with the first record with a key greater than
   * the specified key. If id.isEmpty, return a block starting with the minimum
   * key. Each resulting block should contain only the columns specified in the 
   * column set; if the set of columns is empty, return all columns.
   */
  def getBlockAfter(id: Option[Key], columns: Set[ColumnDescriptor] = Set()): Option[BlockProjectionData[Key, Block]]
}
