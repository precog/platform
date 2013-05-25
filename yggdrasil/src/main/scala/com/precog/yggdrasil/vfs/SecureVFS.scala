package com.precog.yggdrasil
package vfs

import com.precog.common._
import com.precog.common.accounts._
import com.precog.common.ingest._
import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.yggdrasil.execution._
import com.precog.yggdrasil.metadata._
import com.precog.yggdrasil.nihdb._
import com.precog.yggdrasil.scheduling._
import com.precog.util._
import ResourceError._
import Permission._

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
import scalaz.syntax.std.list._
import scalaz.syntax.std.option._
import scalaz.effect.IO
import scala.math.Ordered._

trait VFSMetadata[M[+_]] {
  def findDirectChildren(apiKey: APIKey, path: Path): EitherT[M, ResourceError, Set[Path]]
  def pathStructure(apiKey: APIKey, path: Path, property: CPath, version: Version): EitherT[M, ResourceError, PathStructure]
  def size(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, Long]
}

trait SecureVFSModule[M[+_], Block] extends VFSModule[M, Block] {
  case class StoredQueryResult(data: StreamT[M, Block], cachedAt: Option[Instant])

  class SecureVFS(vfs: VFS, permissionsFinder: PermissionsFinder[M], jobManager: JobManager[M], clock: Clock)(implicit M: Monad[M]) extends VFSMetadata[M] with Logging {
    final val unsecured = vfs

    private def verifyResourceAccess(apiKey: APIKey, path: Path, readMode: ReadMode): Resource => EitherT[M, ResourceError, Resource] = { resource =>
      import AccessMode._
      val permissions: Set[Permission] = resource.authorities.accountIds map { accountId =>
        val writtenBy = WrittenBy(accountId)
        readMode match {
          case Read => ReadPermission(path, writtenBy)
          case Execute => ExecutePermission(path, writtenBy)
          case ReadMetadata => ReducePermission(path, writtenBy)
        }
      }

      EitherT {
        permissionsFinder.apiKeyFinder.hasCapability(apiKey, permissions, Some(clock.now())) map {
          case true => \/.right(resource)
          case false => \/.left(permissionsError("API key %s does not provide %s permission to resource at path %s.".format(apiKey, readMode.name, path.path)))
        }
      }
    }

    private def verifyPathAccess[A](apiKey: APIKey, path: Path, accessMode: AccessMode)(result: => EitherT[M, ResourceError, A]): EitherT[M, ResourceError, A] = { 
      result
    }

    final def readResource(apiKey: APIKey, path: Path, version: Version, readMode: ReadMode): EitherT[M, ResourceError, Resource] = {
      vfs.readResource(path, version) >>= 
      verifyResourceAccess(apiKey, path, readMode)
    }

    final def readQuery(apiKey: APIKey, path: Path, version: Version, readMode: ReadMode): EitherT[M, ResourceError, String] = {
      readResource(apiKey, path, version, readMode) >>= 
      Resource.asQuery(path, version)
    }

    final def readProjection(apiKey: APIKey, path: Path, version: Version, readMode: ReadMode): EitherT[M, ResourceError, Projection] = {
      readResource(apiKey, path, version, readMode) >>= 
      Resource.asProjection(path, version)
    }

    final def size(apiKey: APIKey, path: Path, version: Version): EitherT[M, ResourceError, Long] = {
      readResource(apiKey, path, version, AccessMode.ReadMetadata) flatMap { //need mapM
        _.fold(br => EitherT.right(br.byteLength.point[M]), pr => EitherT.right(pr.recordCount))
      }
    }

    final def pathStructure(apiKey: APIKey, path: Path, selector: CPath, version: Version): EitherT[M, ResourceError, PathStructure] = {
      readProjection(apiKey, path, version, AccessMode.ReadMetadata) >>=
      VFS.pathStructure(selector)
    }

    final def findDirectChildren(apiKey: APIKey, path: Path): EitherT[M, ResourceError, Set[Path]] = {
      verifyPathAccess(apiKey, path, AccessMode.ReadMetadata) {
        for {
          children  <- vfs.findDirectChildren(path)
          permitted <- EitherT.right(permissionsFinder.findBrowsableChildren(apiKey, path))
        } yield { 
          children filter { child => permitted.exists(_.isEqualOrParentOf(path / child)) }
        }
      }
    }

    final def executeStoredQuery(platform: Platform[M, Block, StreamT[M, Block]], scheduler: Scheduler[M], apiKey: APIKey, path: Path, queryOptions: QueryOptions)(implicit M: Monad[M]): EitherT[M, EvaluationError, StoredQueryResult] = {
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
          val recacheAction = (recacheAfter.exists(ms => timestamp.plus(ms) < clock.instant())).whenM[({ type l[a] = EitherT[M, EvaluationError, a] })#l, UUID] {
            // if recacheAfter has expired since the head version was cached,
            // then return the cached version and refresh the cache
            for {
              queryResource <- readResource(apiKey, path, Version.Current, AccessMode.Execute) leftMap storageError
              basePath      <- EitherT((path.prefix \/> invalidState("Path %s cannot be relativized.".format(path.path))).point[M])
              taskId        <- scheduler.addTask(None, apiKey, queryResource.authorities, basePath, path, cachePath, None) leftMap invalidState
            } yield taskId
          } 

          for {
            _          <- recacheAction
            projection <- readProjection(apiKey, cachePath, Version.Current, AccessMode.Read) leftMap storageError
          } yield {
            StoredQueryResult(projection.getBlockStream(None), Some(timestamp))
          }
           
        case Some(VersionEntry(_, _, timestamp)) =>
          logger.debug("Cached entry (%s) found for %s, but is not applicable to max-age %s".format(timestamp, path, maxAge))
          fallBack

        case None => 
          logger.debug("No cached entry found for " + path)
          fallBack
      }
    }

    private def executeUncached(platform: Platform[M, Block, StreamT[M, Block]], apiKey: APIKey, path: Path, queryOptions: QueryOptions, cacheAt: Option[Path])(implicit M: Monad[M]): EitherT[M, EvaluationError, StoredQueryResult] = {
      import EvaluationError._
      logger.debug("Executing query for %s and caching to %s".format(path, cacheAt))
      for { 
        executor <- platform.executorFor(apiKey) leftMap { err => systemError(new RuntimeException(err)) }
        queryRes <- readResource(apiKey, path, Version.Current, AccessMode.Execute) leftMap { storageError _ }
        query    <- Resource.asQuery(path, Version.Current).apply(queryRes) leftMap { storageError _ }
        _ = logger.debug("Text of stored query at %s: \n%s".format(path.path, query))
        basePath <- EitherT(M point { path.prefix \/> invalidState("Path %s cannot be relativized.".format(path.path)) })
        raw      <- executor.execute(apiKey, query, basePath, queryOptions)
        caching  <- cacheAt match {
          case Some(cachePath) =>
            for {
              perms <- EitherT.right(permissionsFinder.writePermissions(apiKey, cachePath, clock.instant()))
              job <- EitherT.right(jobManager.createJob(apiKey, "Cache run for path %s".format(path.path), "Cached query run.", None, Some(clock.now())))
            } yield {
              logger.debug("Building caching stream for path %s writing to %s".format(path.path, cachePath.path))
              val allPerms = Map(apiKey -> perms.toSet[Permission])
              // FIXME: determination of authorities with which to write the cached data needs to be implemented;
              // for right now, simply using the authorities with which the query itself was written is probably
              // best.
              persistingStream(apiKey, cachePath, queryRes.authorities, perms.toSet[Permission], Some(job.id), raw, clock) {
                VFS.derefValue
              }
            }

          case None =>
            logger.debug("No caching to be performed for query results of query at path  %s".format(path.path))
            EitherT.right(raw.point[M])
        }
      } yield {
        StoredQueryResult(caching, None)
      }
    }

    def persistingStream(apiKey: APIKey, path: Path, writeAs: Authorities, perms: Set[Permission], jobId: Option[JobId], stream: StreamT[M, Block], clock: Clock)(blockf: Block => Block): StreamT[M, Block] = {
      val streamId = java.util.UUID.randomUUID()
      val allPerms = Map(apiKey -> perms)

      StreamT.unfoldM((0, stream)) {
        case (pseudoOffset, s) =>
          s.uncons flatMap {
            case Some((x, xs)) =>
              val ingestRecords = VFS.toJsonElements(blockf(x)).zipWithIndex map {
                case (v, i) => IngestRecord(EventId(pseudoOffset, i), v)
              }

              logger.debug("Persisting %d stream records (from slice of size %d) to %s".format(ingestRecords.size, VFS.blockSize(x), path))

              for {
                terminal <- xs.isEmpty
                par <- {
                  // FIXME: is Replace always desired here? Any case
                  // where we might want Create? AFAICT, this is only
                  // used for caching queries right now.
                  val streamRef = StreamRef.Replace(streamId, terminal)
                  val msg = IngestMessage(apiKey, path, writeAs, ingestRecords, jobId, clock.instant(), streamRef)
                  vfs.writeAllSync(Seq((pseudoOffset, msg))).run
                }
              } yield {
                par.fold(
                  errors => {
                    logger.error("Unable to complete persistence of result stream by %s to %s as %s: %s".format(apiKey, path.path, writeAs, errors.shows))
                    None
                  },
                  _ => Some((x, (pseudoOffset + 1, xs)))
                )
              }

            case None =>
              None.point[M]
          }
      }
    }
  }
}


