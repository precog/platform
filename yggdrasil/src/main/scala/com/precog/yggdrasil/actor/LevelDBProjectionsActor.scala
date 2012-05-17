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
package actor

import leveldb._
import com.precog.common._

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

/**
 * Projections actors for LevelDB-backed projections
 */
trait LevelDBProjectionsActorModule extends ProjectionsActorModule[IterableDataset] {
  def newProjectionsActor(descriptorLocator: ProjectionDescriptorLocator, descriptorIO: ProjectionDescriptorIO): ProjectionsActor = {
    new ProjectionsActor(descriptorLocator, descriptorIO) {
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

      protected def projection(descriptor: ProjectionDescriptor): Validation[Throwable, Projection[IterableDataset]] = {
        def initDescriptor(descriptor: ProjectionDescriptor): IO[File] = {
          descriptorLocator(descriptor).flatMap( f => descriptorIO(descriptor).map(_ => f) )
        }

        projections.get(descriptor) map { success[Throwable, Projection[IterableDataset]] } getOrElse {
          for (projection <- LevelDBProjection.forDescriptor(initDescriptor(descriptor).unsafePerformIO, descriptor)) yield {
            // funkiness due to putIfAbsent semantics of returning Some(v) only if k already exists in the map
            projections.putIfAbsent(descriptor, projection) getOrElse projection 
          } 
        }
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
