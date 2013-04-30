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
package com.precog
package yggdrasil
package vfs

import akka.actor.{ActorRef, ActorSystem}
import akka.dispatch.Future
import akka.util.Timeout

import blueeyes.bkka.FutureMonad
import blueeyes.core.http.MimeType
import blueeyes.json.{ JParser, JString, JValue }
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.IsoSerialization._
import blueeyes.json.serialization.Extractor._
import blueeyes.json.serialization.Versioned._
import blueeyes.util.Clock

import com.google.common.util.concurrent.ThreadFactoryBuilder

import com.precog.common.security.{Authorities, PermissionsFinder}
import com.precog.niflheim.NIHDB

import java.io.{IOException, File, FileOutputStream}
import java.util.concurrent.ScheduledThreadPoolExecutor

import scalaz.{NonEmptyList => NEL, _}
import scalaz.effect.IO
import scalaz.syntax.bifunctor._
import scalaz.syntax.monad._
import scalaz.syntax.std.list._

/**
 * Used to access resources. This is needed because opening a NIHDB requires
 * more than just a basedir, but also things like the chef, txLogScheduler, etc.
 * This also goes for blobs, where the metadata log requires the txLogScheduler.
 */
class DefaultResourceBuilder(
  actorSystem: ActorSystem,
  clock: Clock,
  chef: ActorRef,
  cookThreshold: Int,
  storageTimeout: Timeout,
  permissionsFinder: PermissionsFinder[Future],
  txLogSchedulerSize: Int = 20) { // default for now, should come from config in the future

  import ResourceError._

  private final val txLogScheduler = new ScheduledThreadPoolExecutor(txLogSchedulerSize,
    new ThreadFactoryBuilder().setNameFormat("HOWL-sched-%03d").build())

  private implicit val futureMonad = new FutureMonad(actorSystem.dispatcher)

  def ensureDescriptorDir(versionDir: File, authorities: Authorities): IO[File] = IO {
    if (!versionDir.isDirectory && !versionDir.mkdirs) {
      throw new Exception("Failed to create directory for projection: " + versionDir)
    }
    versionDir
  }

  // Resource creation/open and discovery
  def createNIHDB(versionDir: File, authorities: Authorities): IO[Validation[ResourceError, NIHDBResource]] = {
    ensureDescriptorDir(versionDir, authorities) flatMap { nihDir =>
      NIHDB.create(chef, authorities, nihDir, cookThreshold, storageTimeout, txLogScheduler)(actorSystem)
    } map { dbV =>
      fromExtractorError("Failed to create NIHDB") <-: (dbV map (NIHDBResource(_, authorities)(actorSystem)))
    }
  }

  def openNIHDB(descriptorDir: File): IO[ValidationNel[ResourceError, NIHDBResource]] = {
    NIHDB.open(chef, descriptorDir, cookThreshold, storageTimeout, txLogScheduler)(actorSystem).map (_.map { dbV =>
      val resV = dbV map {
        case (authorities, nihdb) => NIHDBResource(nihdb, authorities)(actorSystem)
      }
      fromExtractorErrorNel("Failed to open NIHDB") <-: resV
    }.getOrElse(Failure(NEL(MissingData("No NIHDB projection found in " + descriptorDir)))))
  }

  final val blobMetadataFilename = "blob_metadata"

  def isBlob(versionDir: File): Boolean = (new File(versionDir, blobMetadataFilename)).exists

  /**
   * Open the blob for reading in `baseDir`.
   */
  def openBlob(versionDir: File): IO[ValidationNel[ResourceError, Blob]] = IO {
    val metadataStore = PersistentJValue(versionDir, blobMetadataFilename)
    val metadata = metadataStore.json.validated[BlobMetadata]
    val resource = metadata map { metadata =>
      Blob(new File(versionDir, "data"), metadata)(actorSystem.dispatcher)
    }
    fromExtractorErrorNel("Error reading metadata") <-: resource
  }

  /**
   * Creates a blob from a data stream.
   */
  def createBlob[M[+_]: Monad](versionDir: File, mimeType: MimeType, authorities: Authorities,
      data: StreamT[M, Array[Byte]]): M[Validation[ResourceError, Blob]] = {

    val file = new File(versionDir, "data")
    val out = new FileOutputStream(file)
    def write(size: Long, stream: StreamT[M, Array[Byte]]): M[Validation[ResourceError, Long]] = {
      stream.uncons.flatMap {
        case Some((bytes, tail)) =>
          try {
            out.write(bytes)
            write(size + bytes.length, tail)
          } catch { case (ioe: IOException) =>
            out.close()
            Failure(IOError(ioe)).point[M]
          }

        case None =>
          out.close()
          Success(size).point[M]
      }
    }

    write(0L, data) map (_ flatMap { size =>
      try {
        val metadata = BlobMetadata(mimeType, size, clock.now(), authorities)
        val metadataStore = PersistentJValue(versionDir, blobMetadataFilename)
        metadataStore.json = metadata.serialize
        Success(Blob(file, metadata)(actorSystem.dispatcher))
      } catch { case (ioe: IOException) =>
        Failure(IOError(ioe))
      }
    })
  }
}
