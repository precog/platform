package com.precog.yggdrasil

import scalaz.effect._

trait Projection {
  def descriptor: ProjectionDescriptor

  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit]
}

trait ProjectionsModule {
  type ProjectionImpl <: Projection

  protected def projectionFactory: ProjectionFactory

  trait ProjectionFactory {
    def projection(descriptor: ProjectionDescriptor): IO[ProjectionImpl]

    def close(p: ProjectionImpl): IO[Unit]
  }
}

trait BlockProjection[+Block] extends Projection {
  def getBlockAfter(id: Option[Identities]): Option[Block]
}

trait FullProjection[+Dataset] extends Projection {
  def allRecords(expiresAt: Long): Dataset
}
