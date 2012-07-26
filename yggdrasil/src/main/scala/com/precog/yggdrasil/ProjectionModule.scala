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

trait BlockProjectionLike[Key, Block] extends ProjectionLike {
  implicit def keyOrder: Order[Key]

  case class BlockData(minKey: Key, maxKey: Key, data: Block)

  /** 
   * Get a block of data beginning with the first record with a key greater than
   * the specified key. If id.isSmpty, return a block starting with the minimum
   * key. Each resulting block should contain only the columns specified in the 
   * column set; if the set of columns is empty, return all columns.
   */
  def getBlockAfter(id: Option[Key], columns: Set[ColumnDescriptor] = Set()): Option[BlockData]
}

trait FullProjectionLike[+Dataset] extends ProjectionLike {
  def allRecords(expiresAt: Long): Dataset
}
