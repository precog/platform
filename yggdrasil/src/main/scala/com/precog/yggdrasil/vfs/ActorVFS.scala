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
package vfs

import Resource._
import table.Slice
import metadata.PathStructure

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.niflheim.Reductions
import com.precog.yggdrasil.actor.IngestData
import com.precog.yggdrasil.nihdb.NIHDBProjection
import com.precog.util._

import akka.dispatch.Future
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout

import blueeyes.util.Clock

import java.util.UUID
import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.EitherT._
import scalaz.std.stream._
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._
import scalaz.syntax.std.list._
import scalaz.effect.IO

class ActorVFS(projectionsActor: ActorRef, projectionReadTimeout: Timeout, sliceIngestTimeout: Timeout)(implicit M: Monad[Future]) 
    extends VFS[Future]()(M, new (IO ~> Future) { def apply[A](io: IO[A]) = M.point(io.unsafePerformIO) })  with Logging {

  def writeAll(data: Seq[(Long, EventMessage)]): IO[PrecogUnit] = {
    IO { projectionsActor ! IngestData(data) }
  }

  def writeAllSync(data: Seq[(Long, EventMessage)]): EitherT[Future, ResourceError, PrecogUnit] = EitherT {
    implicit val timeout = sliceIngestTimeout
    for {
      // it's necessary to group by path then traverse since each path will respond to ingest independently.
      // -- a bit of a leak of implementation detail, but that's the actor model for you.
      allResults <- (data groupBy { case (offset, msg) => msg.path }).toStream traverse { case (path, subset) => 
        (projectionsActor ? IngestData(subset)).mapTo[WriteResult] 
      }
    } yield {
      val errors: List[ResourceError] = allResults.toList collect { case PathOpFailure(_, error) => error }
      errors.toNel.map(ResourceError.all).toLeftDisjunction(PrecogUnit)
    }
  }

  def readResource(path: Path, version: Version): EitherT[Future, ResourceError, Resource] = {
    implicit val t = projectionReadTimeout
    EitherT {
      (projectionsActor ? Read(path, version)).mapTo[ReadResult] map {
        case ReadSuccess(_, resource) => \/.right(resource)
        case PathOpFailure(_, error) => \/.left(error)
      }
    }
  }

  // NIHDBProjection works in Future, so although this depends on 
  def readProjection(path: Path, version: Version): EitherT[Future, ResourceError, ProjectionLike[Future, Long, Slice]] = {
    readResource(path, version) flatMap {
      _.fold(
        _ => left(NotFound("Requested resource at %s version %s cannot be interpreted as a Quirrel dataset.".format(path.path, version)).point[Future]),
        pr => right(pr.projection)
      )
    }
  }

  def findDirectChildren(path: Path): Future[Set[Path]] = {
    implicit val t = projectionReadTimeout
    val paths = (projectionsActor ? FindChildren(path)).mapTo[Set[Path]]
    paths map { _ flatMap { _ - path }}
  }

  def currentStructure(path: Path, selector: CPath): EitherT[Future, ResourceError, PathStructure] = {
    readProjection(path, Version.Current) flatMap { projection => 
      right {
        for (children <- projection.structure) yield {
          PathStructure(projection.reduce(Reductions.count, selector), children.map(_.selector))
        }
      }
    }
  }

  def currentVersion(path: Path) = {
    implicit val t = projectionReadTimeout
    (projectionsActor ? CurrentVersion(path)).mapTo[Option[VersionEntry]]
  }
}
