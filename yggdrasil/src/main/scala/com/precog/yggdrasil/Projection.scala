package com.precog.yggdrasil

import scalaz.effect._

trait ProjectionFactory[Dataset] {
  type ProjectionImpl <: Projection[Dataset]

  def projection(descriptor: ProjectionDescriptor): IO[ProjectionImpl]

  def close(p: ProjectionImpl): IO[Unit]
}

trait Projection[Dataset] {
  def descriptor: ProjectionDescriptor

  def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): IO[Unit]

  def allRecords(expiresAt: Long) : Dataset
}
