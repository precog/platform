package com.precog.yggdrasil

import scalaz.Order
import scalaz.effect._

trait ProjectionModule {
  type Projection <: ProjectionLike

  val Projection: ProjectionCompanion

  trait ProjectionCompanion {
    def open(descriptor: ProjectionDescriptor): IO[Projection]

    def close(p: Projection): IO[Unit]
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

  implicit def keyOrder: Order[Key]

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
