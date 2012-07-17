package com.precog.yggdrasil

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

trait BlockProjectionLike[+Block] extends ProjectionLike {
  def getBlockAfter(id: Option[Identities]): Option[Block]
}

trait FullProjectionLike[+Dataset] extends ProjectionLike {
  def allRecords(expiresAt: Long): Dataset
}
