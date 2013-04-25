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
