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
