package com.precog.yggdrasil
package actor

import leveldb._
import com.precog.common._

import akka.actor.ActorRef
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.duration._
import akka.util.Timeout

import blueeyes.json.JsonAST._
import blueeyes.persistence.cache.Cache
import blueeyes.persistence.cache.CacheSettings
import blueeyes.persistence.cache.ExpirationPolicy

import com.weiglewilczek.slf4s._

import java.io.File
import java.util.concurrent.TimeUnit

import scala.annotation.tailrec
import scala.collection.mutable

import scalaz._
import scalaz.Validation._
import scalaz.effect._
import scalaz.syntax.std.optionV._

/**
 * Projections actors for LevelDB-backed projections
 */
trait LevelDBProjectionsActorModule extends ProjectionsActorModule[IterableDataset] {
  def newProjectionsActor(metadataActor: ActorRef, timeout: Timeout): ProjectionsActor = {
    new ProjectionsActor(metadataActor, timeout) with Logging {
      private val projectionCacheSettings = CacheSettings(
        expirationPolicy = ExpirationPolicy(Some(2), Some(2), TimeUnit.MINUTES), 
        evict = (descriptor: ProjectionDescriptor, projection: LevelDBProjection) => projection.close.unsafePerformIO
      )

      // Cache control map that stores reference counts for projection descriptors managed by the cache
      private val outstandingReferences = mutable.Map.empty[ProjectionDescriptor, Int]

      // Cache for active projections
      private val projections = Cache.concurrentWithCheckedEviction[ProjectionDescriptor, LevelDBProjection](projectionCacheSettings) {
        (descriptor, _) => outstandingReferences.get(descriptor) forall { _ == 0 }
      }

      protected def status =  JObject(JField("Projections", JObject(JField("cacheSize", JInt(projections.size)) :: 
                                                                    JField("outstandingReferences", JInt(outstandingReferences.size)) :: Nil)) :: Nil)

      protected def projection(base: Option[File], descriptor: ProjectionDescriptor): Validation[Throwable, Projection[IterableDataset]] = base match {
        case Some(root) =>
          logger.debug("Obtaining LevelDB projection for " + descriptor + " from " + root)
          projections.get(descriptor) map { success[Throwable, Projection[IterableDataset]] } getOrElse {
            for (projection <- LevelDBProjection.forDescriptor(root, descriptor)) yield {
              // funkiness due to putIfAbsent semantics of returning Some(v) only if k already exists in the map
              projections.putIfAbsent(descriptor, projection) getOrElse projection 
            }
          }

        case None => 
          logger.error("No base for projection")
          Failure(new java.io.FileNotFoundException("Could not locate base for " + descriptor))
      }

      protected def reserved(descriptor: ProjectionDescriptor): Unit = {
        val current = outstandingReferences.getOrElse(descriptor, 0)
        outstandingReferences += (descriptor -> (current + 1))
      }

      protected def released(descriptor: ProjectionDescriptor): Unit = {
        outstandingReferences.get(descriptor) match {
          case Some(current) if current > 1 => 
            outstandingReferences += (descriptor -> (current - 1))

          case Some(current) if current == 1 => 
            outstandingReferences -= descriptor

          case None =>
            logger.warn("Extraneous request to release reference to projection descriptor " + descriptor + 
                        "; no outstanding references for this descriptor recorded.")
        }
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
