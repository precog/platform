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
package nihdb

import akka.dispatch.Future

import com.precog.common._
import com.precog.common.security.Authorities
import com.precog.niflheim._
import com.precog.yggdrasil.table.Slice

import scalaz.{NonEmptyList => NEL, Monad}

trait NIHDBProjection extends ProjectionLike[Future, Long, Slice] {
  def reduce[A](reduction: Reduction[A], path: CPath): Map[CType, A]
}

object NIHDBProjection {
  def wrap(nihdb: NIHDB)(implicit M: Monad[Future]) : Future[NIHDBProjection] = {
    for {
      authorities <- nihdb.authorities
      proj <- wrap(nihdb, authorities)
    } yield proj
  }

  def wrap(nihdb: NIHDB, authorities: Authorities)(implicit M: Monad[Future]) : Future[NIHDBProjection] = {
    nihdb.getSnapshot.map { snap =>
      (new NIHDBAggregate(NEL(snap), authorities)).projection
    }
  }
}
