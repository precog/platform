package com.precog.yggdrasil
package vfs

import com.precog.common._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.yggdrasil.execution._
import com.precog.yggdrasil.nihdb._
import com.precog.yggdrasil.scheduling._
import com.precog.yggdrasil.table.Slice
import com.precog.util.InstantOrdering
import Resource._

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

case class StoredQueryResult[M[+_]](data: StreamT[M, Slice], cachedAt: Option[Instant])

class SecureVFS[M[+_]](vfs: VFS[M], permissionsFinder: PermissionsFinder[M], jobManager: JobManager[M], scheduler: Scheduler[M], clock: Clock) extends Logging {

  def readResource(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, Resource] = {
    //FIXME: Unsecured for now
    vfs.readResource(path, version)
  }

  def readQuery(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, String] = {
    vfs.readQuery(path, version)
  }

  def readProjection(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, ProjectionLike[M, Long, Slice]] = {
    vfs.readProjection(path, version)
  }

  def findDirectChildren(apiKey: APIKey, path: Path)(implicit M: Monad[M]) = {
    for {
      permitted <- permissionsFinder.findBrowsableChildren(apiKey, path) 
      children <- vfs.findDirectChildren(path) 
    } yield {
      children filter { child => permitted.exists(_.isEqualOrParentOf(path / child)) }
    }
  }

  def executeStoredQuery(platform: Platform[M, StreamT[M, Slice]], apiKey: APIKey, path: Path, queryOptions: QueryOptions)(implicit M: Monad[M]): M[Validation[EvaluationError, StoredQueryResult[M]]] = {
    import queryOptions.cacheControl._
    val cachePath = path / Path(".cached")
    def fallBack = if (onlyIfCached) {
      // Return a 202 and schedule a caching run of the query
      sys.error("todo")
    } else {
      // if current cached version is older than the max age or cached
      // version does not exist, then run synchronously and cache (if cacheable)
      executeUncached(platform, apiKey, path, queryOptions, cacheable.option(cachePath)).run.map(_.validation)
    }

    logger.debug("Checking on cached result for %s with maxAge = %s and recacheAfter = %s and cacheable = %s".format(path, maxAge, recacheAfter, cacheable))
    platform.metadataClient.currentVersion(apiKey, cachePath) flatMap {
      case Some(VersionEntry(id, _, timestamp)) if maxAge.forall(ms => timestamp.plus(ms) >= clock.instant()) =>
        logger.debug("Found fresh cache entry (%s) for query on %s".format(timestamp, path))
        if (recacheAfter.exists(ms => timestamp.plus(ms) < clock.instant())) {
          // if recacheAfter has expired since the head version was cached,
          // then return the cached version and refresh the cache
          for {
            writeAsOpt <- platform.metadataClient.currentAuthorities(apiKey, path)
            writeAsV = writeAsOpt.toSuccess(nels("Unable to determine write authorities for caching of %s".format(path.path)))
            basePathV = path.prefix.toSuccess(nels("Path %s cannot be relativized.".format(path.path)))
            taskIdV <- (writeAsV tuple basePathV) traverse { 
              case (writeAs, basePath) =>
                scheduler.addTask(None, apiKey, writeAs, basePath, path, cachePath, None).map(_.toValidationNel)
            } 
            currentV <- readProjection(apiKey, cachePath, Version.Current).run map { 
              _.validation leftMap { err => nels(err.toString) } //TODO: obviously suboptimal
            }
          } yield {
            (taskIdV.flatMap(a => a) |@| currentV) { (_, projection) => 
              // FIXME: because we're getting data back from
              // NIHDBProjection, it contains the identities. Is there
              // a better way/place to do this deref?
              StoredQueryResult(projection.getBlockStream(None), Some(timestamp))
            } leftMap { errors =>
              EvaluationError.systemError {
                new RuntimeException("Errors occurred running previously cached query: " + errors.shows)
              }
            }
          }
        } else {
          // simply return the cached results
          val cachedResults = readProjection(apiKey, cachePath, Version.Current) map { proj =>
            StoredQueryResult(proj.getBlockStream(None), Some(timestamp))
          }

          cachedResults.run.map(_.validation.leftMap(StorageError(_)))
        }
         
      case Some(VersionEntry(_, _, timestamp)) =>
        logger.debug("Cached entry (%s) found for %s, but is not applicable to max-age %s".format(timestamp, path, maxAge))
        fallBack

      case None => 
        logger.debug("No cached entry found for " + path)
        fallBack
    }
  }

  private def executeUncached(platform: Platform[M, StreamT[M, Slice]], apiKey: APIKey, path: Path, queryOptions: QueryOptions, cacheAt: Option[Path])(implicit M: Monad[M]): EitherT[M, EvaluationError, StoredQueryResult[M]] = {
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
            writeAs <- EitherT {
              platform.metadataClient.currentAuthorities(apiKey, cachePath) map {
                _.toRightDisjunction(storageError(PermissionsError("Unable to determine write authorities for caching of %s".format(path))))
              }
            }
            perms <- EitherT.right(permissionsFinder.writePermissions(apiKey, cachePath, clock.instant()))
            job <- EitherT.right(jobManager.createJob(apiKey, "Cache run for path %s".format(path.path), "Cached query run.", None, Some(clock.now())))
          } yield {
            val allPerms = Map(apiKey -> perms.toSet[Permission])
            vfs.persistingStream(apiKey, cachePath, writeAs, perms.toSet[Permission], Some(job.id), raw) 
          }

        case None =>
          EitherT.right(raw.point[M])
      }
    } yield {
      StoredQueryResult(caching, None)
    }
  }

}



