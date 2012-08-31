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
package jdbm3

import iterable.IterableDataset
import table._
import com.precog.util.FileOps

import org.joda.time.DateTime

import java.io.File
import java.nio.ByteBuffer
import java.util.concurrent.{Executors,TimeoutException}

import scala.collection.Iterator
import scalaz.NonEmptyList
import scalaz.Validation
import scalaz.effect._
import scalaz.syntax.validation._

import com.weiglewilczek.slf4s.Logger

trait JDBMProjectionModule extends ProjectionModule {
  val pmLogger = Logger("JDBMProjectionModule")

  type Key = Identities
  class Projection private[JDBMProjectionModule] (baseDir: File, descriptor: ProjectionDescriptor) extends JDBMProjection(baseDir, descriptor) {
    def traverseIndex(expiresAt: Long): IterableDataset[Seq[CValue]] = allRecords(expiresAt)
  }

  trait JDBMProjectionCompanion extends ProjectionCompanion {
    def fileOps: FileOps

    def baseDir(descriptor: ProjectionDescriptor): File
    
    def archiveDir(descriptor: ProjectionDescriptor): File

    def open(descriptor: ProjectionDescriptor): IO[Projection] = {
      pmLogger.debug("Opening JDBM projection for " + descriptor)
      val base = baseDir(descriptor)
      val baseDirV: IO[File] = 
        fileOps.exists(base) flatMap { 
          case true  => IO(base)
          case false => fileOps.mkdir(base) map {
                          case true  => base
                          case false => throw new RuntimeException("Could not create database basedir " + base)
                        }
        }

      baseDirV map { (bd: File) => new Projection(bd, descriptor) }
    }

    def close(projection: Projection) = {
      pmLogger.debug("Requesting close on " + projection)
      IO(projection.close())
    }
    
    def archive(projection: Projection) = {
      pmLogger.debug("Archiving " + projection)
      val descriptor = projection.descriptor
      close(projection).flatMap(_ => fileOps.rename(baseDir(descriptor), archiveDir(descriptor)))
    }
  }
}
