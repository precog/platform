package com.precog.shard

import com.precog.daze._
import com.precog.common._

import com.precog.common.security._
import com.precog.common.jobs._
import com.precog.muspelheim._
import com.precog.yggdrasil.table.Slice
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

import scala.math.Ordered._
import scalaz._
import scalaz.NonEmptyList.nels
import scalaz.Validation.{ success, failure }
import scalaz.std.stream._
import scalaz.syntax.apply._
import scalaz.syntax.applicative._
import scalaz.syntax.monad._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._


object StoredQueryExecutor {
  def cachePath(path: Path) = path / Path(".cached")
}

class StoredQueryExecutor[M[+_]: Monad](platform: Platform[M, StreamT[M, Slice]], vfs: VFS[M], jobManager: JobManager[M], permissionsFinder: PermissionsFinder[M], clock: Clock) {
  import StoredQueryExecutor._

  def executeStoredQuery(apiKey: APIKey, path: Path, queryOptions: QueryOptions, maxAge: Option[Long], recacheAfter: Option[Long], cacheable: Boolean): M[Validation[EvaluationError, StreamT[M, Slice]]] = {
    val cachedPath = cachePath(path)
    platform.metadataClient.currentVersion(apiKey, cachedPath) flatMap {
      case Some(VersionEntry(id, _, timestamp)) if maxAge.forall(ms => timestamp.plus(ms) >= clock.instant()) =>
        if (recacheAfter.exists(ms => timestamp.plus(ms) < clock.instant())) {
          // if recacheAfter has expired since the head version was cached,
          // then return the cached version and refresh the cache
          sys.error("todo")
        } else {
          // simply return the cached results
          sys.error("todo")
        }
         
      case _ => 
        // if current cached version is older than the max age or cached
        // version does not exist, then run synchronously and cache (if cacheable)
        for { 
          executorV <- platform.executorFor(apiKey)
          // TODO: Check read permissions
          queryOpt <- vfs.readQuery(path, Version.Current)
          queryV = queryOpt.toSuccess("Query not found at %s for %s".format(path.path, apiKey))
          resultV <- (queryV.toValidationNel tuple executorV.toValidationNel) traverse { 
            case (query, executor) => executor.execute(apiKey, query, path, queryOptions) 
          } 
          resultV0 = resultV.leftMap(e => EvaluationError.acc(e map { InvalidStateError(_)})).flatMap(a => a)
          cachingV <- if (cacheable) {
            for {
              writeAs <- platform.metadataClient.currentAuthorities(apiKey, path)
              perms <- permissionsFinder.writePermissions(apiKey, cachedPath, clock.instant())
              job <- jobManager.createJob(apiKey, "Cache run for path %s".format(path.path), "Cached query run.", None, Some(clock.now()))
            } yield {
              val allPerms = Map(apiKey -> perms.toSet[Permission])
              val writeAsV = writeAs.toSuccess("Unable to determine write authorities for caching of %s".format(path))
              for {
                writeAs <- writeAsV.leftMap(InvalidStateError(_))
                stream <- resultV0
              } yield { 
                vfs.persistingStream(apiKey, cachedPath, writeAs, perms.toSet[Permission], Some(job.id), stream) 
              }
            }
          } else {
            resultV0.point[M]
          }
        } yield cachingV
    }
  }
}



