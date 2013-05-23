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

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.yggdrasil.execution._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.nihdb._
import com.precog.yggdrasil.scheduling._
import com.precog.util._
import ResourceError._

import akka.dispatch.Future
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout

import blueeyes.util.Clock

import java.util.UUID
import org.joda.time.Instant
import com.weiglewilczek.slf4s.Logging

import scalaz._
import scalaz.NonEmptyList.nels
import scalaz.std.string._
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._
import scalaz.effect.IO
import scala.math.Ordered._

trait SecureVFSModule[M[+_], Key, Block] extends VFSModule[M, Key, Block] {
  case class StoredQueryResult(data: StreamT[M, Block], cachedAt: Option[Instant])

  trait VFSMetadata {
    // TODO: Find the right balance of abstraction here.
    def findDirectChildren(apiKey: APIKey, path: Path): M[Set[Path]]
    def structure(apiKey: APIKey, path: Path, property: CPath, version: Version): EitherT[M, ResourceError, PathStructure]
    def size(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, Long]
  }

  class SecureVFS(vfs: VFS, permissionsFinder: PermissionsFinder[M], jobManager: JobManager[M], scheduler: Scheduler[M], clock: Clock)(implicit M: Monad[M]) extends VFSMetadata with Logging {
    def writeAll(data: Seq[(Long, EventMessage)]): IO[PrecogUnit] = {
      //FIXME: Unsecured for now
      vfs.writeAll(data)
    }

    def readResource(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, Resource] = {
      //FIXME: Unsecured for now
      vfs.readResource(path, version)
    }

    def readQuery(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, String] = {
      //FIXME: Unsecured for now
      vfs.readQuery(path, version)
    }

    def readProjection(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, Projection] = {
      //FIXME: Unsecured for now
      vfs.readProjection(path, version)
    }

    def size(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, Long] = {
      readResource(apiKey, path, version) flatMap { //need mapM
        _.fold(br => EitherT.right(br.metadata.size.point[M]), pr => EitherT.right(pr.length))
      }
    }

    def structure(apiKey: APIKey, path: Path, property: CPath, version: Version): EitherT[M, ResourceError, PathStructure] = {
      sys.error("todo")
    }

    def findDirectChildren(apiKey: APIKey, path: Path): M[Set[Path]] = {
      ^(vfs.findDirectChildren(path), permissionsFinder.findBrowsableChildren(apiKey, path)) { (children, permitted) =>
        children filter { child => permitted.exists(_.isEqualOrParentOf(path / child)) }
      }
    }

    def executeStoredQuery(platform: Platform[M, StreamT[M, Block]], apiKey: APIKey, path: Path, queryOptions: QueryOptions)(implicit M: Monad[M]): EitherT[M, EvaluationError, StoredQueryResult] = {
      import queryOptions.cacheControl._
      val cachePath = path / Path(".cached")
      def fallBack = if (onlyIfCached) {
        // Return a 202 and schedule a caching run of the query
        sys.error("todo")
      } else {
        // if current cached version is older than the max age or cached
        // version does not exist, then run synchronously and cache (if cacheable)
        executeUncached(platform, apiKey, path, queryOptions, cacheable.option(cachePath))
      }

      logger.debug("Checking on cached result for %s with maxAge = %s and recacheAfter = %s and cacheable = %s".format(path, maxAge, recacheAfter, cacheable))
      EitherT.right(vfs.currentVersion(cachePath)) flatMap {
        case Some(VersionEntry(id, _, timestamp)) if maxAge.forall(ms => timestamp.plus(ms) >= clock.instant()) =>
          import EvaluationError._

          logger.debug("Found fresh cache entry (%s) for query on %s".format(timestamp, path))
          if (recacheAfter.exists(ms => timestamp.plus(ms) < clock.instant())) {
            // if recacheAfter has expired since the head version was cached,
            // then return the cached version and refresh the cache
            for {
              queryResource <- readResource(apiKey, path, Version.Current) leftMap storageError
              basePath      <- EitherT((path.prefix \/> invalidState("Path %s cannot be relativized.".format(path.path))).point[M])
              taskId     <- scheduler.addTask(None, apiKey, queryResource.authorities, basePath, path, cachePath, None) leftMap invalidState
              projection <- readProjection(apiKey, cachePath, Version.Current) leftMap storageError
            } yield {
              // FIXME: because we're getting data back from
              // NIHDBProjection, it contains the identities. Is there
              // a better way/place to do this deref?
              StoredQueryResult(projection.getBlockStream(None), Some(timestamp))
            }
          } else {
            // simply return the cached results
            readProjection(apiKey, cachePath, Version.Current) map { proj =>
              StoredQueryResult(proj.getBlockStream(None), Some(timestamp))
            } leftMap { 
              StorageError(_) 
            }
          }
           
        case Some(VersionEntry(_, _, timestamp)) =>
          logger.debug("Cached entry (%s) found for %s, but is not applicable to max-age %s".format(timestamp, path, maxAge))
          fallBack

        case None => 
          logger.debug("No cached entry found for " + path)
          fallBack
      }
    }

    private def executeUncached(platform: Platform[M, StreamT[M, Block]], apiKey: APIKey, path: Path, queryOptions: QueryOptions, cacheAt: Option[Path])(implicit M: Monad[M]): EitherT[M, EvaluationError, StoredQueryResult] = {
      import EvaluationError._
      logger.debug("Executing query for %s and caching to %s".format(path, cacheAt))
      for { 
        executor <- platform.executorFor(apiKey) leftMap { err => systemError(new RuntimeException(err)) }
        query    <- readQuery(apiKey, path, Version.Current) leftMap { storageError _ }
        basePath <- EitherT(M point { path.prefix \/> invalidState("Path %s cannot be relativized.".format(path.path)) })
        raw      <- executor.execute(apiKey, query, basePath, queryOptions) 
        caching  <- cacheAt match {
          case Some(cachePath) =>
            for {
              resource <- readResource(apiKey, cachePath, Version.Current).leftMap(StorageError(_))
              perms <- EitherT.right(permissionsFinder.writePermissions(apiKey, cachePath, clock.instant()))
              job <- EitherT.right(jobManager.createJob(apiKey, "Cache run for path %s".format(path.path), "Cached query run.", None, Some(clock.now())))
            } yield {
              val allPerms = Map(apiKey -> perms.toSet[Permission])
              vfs.persistingStream(apiKey, cachePath, resource.authorities, perms.toSet[Permission], Some(job.id), raw, clock) 
            }

          case None =>
            EitherT.right(raw.point[M])
        }
      } yield {
        StoredQueryResult(caching, None)
      }
    }
  }
}


