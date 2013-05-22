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
package com.precog.shard

import scheduling.Scheduler
import com.precog.daze._
import com.precog.common._

import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.muspelheim._
import com.precog.yggdrasil.table.Slice
import com.precog.yggdrasil.TransSpecModule
import com.precog.yggdrasil.vfs._
import com.precog.util.InstantOrdering

import blueeyes.core.data._
import blueeyes.core.http._
import blueeyes.core.http.HttpStatusCodes._
import blueeyes.core.service._
import blueeyes.json._
import blueeyes.json.serialization.SerializationImplicits._
import blueeyes.util.Clock

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import com.weiglewilczek.slf4s.Logging

import java.nio.CharBuffer
import java.io.{ StringWriter, PrintWriter }

import org.joda.time.Instant

import scala.math.Ordered._
import scalaz._
import scalaz.NonEmptyList.nels
import scalaz.Validation.{ success, failure }
import scalaz.std.stream._
import scalaz.std.string._
import scalaz.syntax.apply._
import scalaz.syntax.applicative._
import scalaz.syntax.monad._
import scalaz.syntax.show._
import scalaz.syntax.traverse._
import scalaz.syntax.std.boolean._
import scalaz.syntax.std.option._

case class StoredQueryResult[M[+_]](data: StreamT[M, Slice], cachedAt: Option[Instant])

trait StoredQueries[M[+_]] {
  def executeStoredQuery(context: EvaluationContext, queryOptions: QueryOptions, maxAge: Option[Long], recacheAfter: Option[Long], cacheable: Boolean, onlyIfCached: Boolean): M[Validation[EvaluationError, StoredQueryResult[M]]]
}

object VFSStoredQueries {
  def cachePath(path: Path) = path / Path(".cached")
}

class VFSStoredQueries[M[+_]: Monad](platform: Platform[M, StreamT[M, Slice]], vfs: VFS[M], scheduler: Scheduler[M], jobManager: JobManager[M], permissionsFinder: PermissionsFinder[M], clock: Clock) extends StoredQueries[M] with Logging {
  import VFSStoredQueries._

  private def executeUncached(context: EvaluationContext, queryOptions: QueryOptions, cacheAt: Option[Path]): M[Validation[EvaluationError, StoredQueryResult[M]]] = {
    val path = context.basePath
    logger.debug("Executing query for %s and caching to %s".format(path, cacheAt))
    for {
      executorV <- platform.executorFor(context.apiKey)
      // TODO: Check read permissions
      queryOpt <- vfs.readQuery(path, Version.Current)
      queryV = queryOpt.toSuccess(nels("Query not found at %s for %s".format(path.path, context.apiKey)))
      basePathV = path.prefix.toSuccess(nels("Path %s cannot be relativized.".format(path.path)))
      contextV = basePathV map (path0 => context.copy(basePath = path0))
      resultV <- (queryV tuple contextV tuple executorV.toValidationNel) traverse {
        case ((query, context), executor) => executor.execute(query, context, queryOptions)
      }
      resultV0 = resultV.leftMap(e => EvaluationError.acc(e map { InvalidStateError(_)})).flatMap(a => a)
      cachingV <- cacheAt map { cachedPath =>
        for {
          writeAs <- platform.metadataClient.currentAuthorities(context.apiKey, path)
          perms <- permissionsFinder.writePermissions(context.apiKey, cachedPath, clock.instant())
          job <- jobManager.createJob(context.apiKey, "Cache run for path %s".format(path.path), "Cached query run.", None, Some(clock.now()))
        } yield {
          val allPerms = Map(context.apiKey -> perms.toSet[Permission])
          val writeAsV = writeAs.toSuccess("Unable to determine write authorities for caching of %s".format(path))
          for {
            writeAs <- writeAsV.leftMap(InvalidStateError(_))
            stream <- resultV0
          } yield {
            vfs.persistingStream(context.apiKey, cachedPath, writeAs, perms.toSet[Permission], Some(job.id), stream)
          }
        }
      } getOrElse {
        resultV0.point[M]
      }
    } yield cachingV.map(StoredQueryResult(_, None))
  }

  def executeStoredQuery(context: EvaluationContext, queryOptions: QueryOptions, maxAge: Option[Long], recacheAfter: Option[Long], cacheable: Boolean, onlyIfCached: Boolean): M[Validation[EvaluationError, StoredQueryResult[M]]] = {
    val path = context.basePath
    val apiKey = context.apiKey
    val cachedPath = cachePath(path)

    def fallBack = if (onlyIfCached) {
      // Return a 202 and schedule a caching run of the query
      sys.error("todo")
    } else {
      // if current cached version is older than the max age or cached
      // version does not exist, then run synchronously and cache (if cacheable)
      executeUncached(context, queryOptions, cacheable.option(cachedPath))
    }

    logger.debug("Checking on cached result for %s with maxAge = %s and recacheAfter = %s and cacheable = %s".format(path, maxAge, recacheAfter, cacheable))

    platform.metadataClient.currentVersion(apiKey, cachedPath) flatMap {
      case Some(VersionEntry(id, _, timestamp)) if maxAge.forall(ms => timestamp.plus(ms) >= clock.instant()) =>
        logger.debug("Found fresh cache entry (%s) for query on %s".format(timestamp, path))
        if (recacheAfter.exists(ms => timestamp.plus(ms) < clock.instant())) {
          // if recacheAfter has expired since the head version was cached,
          // then return the cached version and refresh the cache
          for {
            writeAsOpt <- platform.metadataClient.currentAuthorities(apiKey, path)
            writeAsV = writeAsOpt.toSuccess(nels("Unable to determine write authorities for caching of %s".format(path.path)))
            basePathV = path.prefix.toSuccess(nels("Path %s cannot be relativized.".format(path.path)))
            contextV = basePathV map (path0 => context.copy(basePath = path0))
            taskIdV <- (writeAsV tuple contextV) traverse {
              case (writeAs, context) =>
                scheduler.addTask(None, apiKey, writeAs, context, path, cachedPath, None).map(_.toValidationNel)
            }
            currentV <- vfs.readCache(cachedPath, Version.Current) map {
              _.toSuccess(nels("Could not find cached data for path %s.".format(cachedPath)))
            }
          } yield {
            // FIXME: what should we be doing with the task ID here? Return in a header?
            (taskIdV.flatMap(a => a) |@| currentV) { (_, stream) =>
              // FIXME: because we're getting data back from
              // NIHDBProjection, it contains the identities. Is there
              // a better way/place to do this deref?
              StoredQueryResult(stream.map(_.deref(TransSpecModule.paths.Value)), Some(timestamp))
            } leftMap { errors =>
              EvaluationError.systemError {
                new RuntimeException("Errors occurred running previously cached query: " + errors.shows)
              }
            }
          }
        } else {
          // simply return the cached results
          vfs.readCache(cachedPath, Version.Current) map {
            _.map { stream =>
              StoredQueryResult(stream.map(_.deref(TransSpecModule.paths.Value)), Some(timestamp))
            }.toSuccess(InvalidStateError("Could not find cached data for path %s.".format(cachedPath)))
          }
        }

      case Some(VersionEntry(_, _, timestamp)) =>
        logger.debug("Cached entry (%s) found for %s, but is not applicable to max-age %s".format(timestamp, path, maxAge))
        fallBack

      case _ =>
        logger.debug("No cached entry found for " + path)
        fallBack
    }
  }
}


object NoopStoredQueries {
  def apply[M[+_]: Monad] = new NoopStoredQueries[M]
}

class NoopStoredQueries[M[+_]: Monad] extends StoredQueries[M] {
  def executeStoredQuery(context: EvaluationContext, queryOptions: QueryOptions, maxAge: Option[Long], recacheAfter: Option[Long], cacheable: Boolean, onlyIfCached: Boolean): M[Validation[EvaluationError, StoredQueryResult[M]]] = sys.error("Stored query execution is not supported.")
}
