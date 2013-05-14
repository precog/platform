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

import table.Slice

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.jobs._

import akka.dispatch.Future
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout

import blueeyes.util.Clock

import java.util.UUID
import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.effect.IO

sealed trait Version
object Version {
  case object Current extends Version
  case class Archived(uuid: UUID) extends Version
}

/** 
 * VFS is an unsecured interface to the virtual filesystem; validation must be performed higher in the stack.
 */
trait VFS[M[+_]] {
  def readQuery(path: Path, version: Version): M[Option[String]]  
  def readCache(path: Path, version: Version): M[Option[StreamT[M, Slice]]]
  def persistingStream(apiKey: APIKey, path: Path, writeAs: Authorities, perms: Set[Permission], jobId: Option[JobId], stream: StreamT[M, Slice]): StreamT[M, Slice] 
}

class ActorVFS(projectionsActor: ActorRef, clock: Clock, projectionReadTimeout: Timeout, sliceIngestTimeout: Timeout)(implicit M: Monad[Future]) extends VFS[Future] with Logging {
  def readQuery(path: Path, version: Version): Future[Option[String]] = {
    implicit val t = projectionReadTimeout
    (projectionsActor ? Read(path, version, None)).mapTo[ReadResult] map {
      case ReadSuccess(_, Some(blob: Blob)) =>
        blob.asString map(Some(_)) except { case t: Throwable => IO { logger.error("Query read error", t); None } } unsafePerformIO

      case failure => 
        logger.warn("Unable to read query at path %s with version %s: %s".format(path.path, version, failure))
        None
    }
  }

  def readCache(path: Path, version: Version): Future[Option[StreamT[Future, Slice]]] = {
    implicit val t = projectionReadTimeout
    (projectionsActor ? ReadProjection(path, version, None)).mapTo[ReadProjectionResult] map {
      case ReadProjectionSuccess(_, Some(projection)) => Some(projection.getBlockStream(None))
      case failure => 
        logger.warn("Unable to read projection at path %s with version %s: %s".format(path.path, version, failure))
        None
    }
  }

  def persistingStream(apiKey: APIKey, path: Path, writeAs: Authorities, perms: Set[Permission], jobId: Option[JobId], stream: StreamT[Future, Slice]): StreamT[Future, Slice] = {
    implicit val askTimeout = sliceIngestTimeout
    val streamId = java.util.UUID.randomUUID()
    val allPerms = Map(apiKey -> perms)

    StreamT.unfoldM((0, stream)) { 
      case (pseudoOffset, s) => 
        s.uncons flatMap {
          case Some((x, xs)) =>
            val ingestRecords = x.toJsonElements.zipWithIndex map { 
              case (v, i) => IngestRecord(EventId(pseudoOffset, i), v)
            }

            for {
              terminal <- xs.isEmpty
              par <- {
                val streamRef = StreamRef.Create(streamId, terminal)
                val msg = IngestMessage(apiKey, path, writeAs, ingestRecords, jobId, clock.instant(), streamRef)
                (projectionsActor ? IngestBundle(Seq((pseudoOffset, msg)), allPerms)).mapTo[PathActionResponse]
              }
            } yield {
              par match {
                case UpdateSuccess(_) => 
                  Some((x, (pseudoOffset + 1, xs)))
                case PathFailure(_, errors) =>
                  logger.error("Unable to complete persistence of result stream by %s to %s as %s: %s".format(apiKey, path.path, writeAs, errors.shows))
                  None
                case UpdateFailure(_, errors) =>
                  logger.error("Unable to complete persistence of result stream by %s to %s as %s: %s".format(apiKey, path.path, writeAs, errors.shows))
                  None
              }
            }

          case None => 
            None.point[Future]
        }
    }
  }
}

