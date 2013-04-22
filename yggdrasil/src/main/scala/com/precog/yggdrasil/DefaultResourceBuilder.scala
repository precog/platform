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


  /**
    * Compute the dir for a given NIHDB projection based on the provided authorities.
    */
  def descriptorDir(versionDir: File, authorities: Authorities): File = {
    //The projections are stored in a folder based on their authority's hash
    new File(versionDir, authorities.sha1)
  }

  /**
    * Enumerate the NIHDB descriptors found under a given base directory
    */
  def findDescriptorDirs(versionDir: File): Option[Set[File]] = {
    // No pathFileFilter needed here, since the projections dir should only contain descriptor dirs
    Option(versionDir.listFiles).map {
      _.filter { dir =>
        dir.isDirectory && NIHDBActor.hasProjection(dir)
      }.toSet
    }
  }

  // Must return a directory
  def ensureDescriptorDir(versionDir: File, authorities: Authorities): IO[File] = IO {
    val dir = descriptorDir(versionDir, authorities)
    if (!dir.exists && !dir.mkdirs()) {
      throw new Exception("Failed to create directory for projection: " + dir)
    }
    dir
  }

  // Resource creation/open and discovery
  def createNIHDB(versionDir: File, authorities: Authorities): IO[Validation[ResourceError, NIHDBResource]] = {
    ensureDescriptorDir(versionDir, authorities).flatMap { nihDir =>
      NIHDB.create(chef, authorities, nihDir, cookThreshold, storageTimeout, txLogScheduler)(actorSystem)
    }.map { dbV =>
      fromExtractorError("Failed to create NIHDB") <-: (dbV map (NIHDBResource(_)(actorSystem.dispatcher)))
    }
  }

  def openNIHDB(versionDir: File): IO[Validation[ResourceError, NIHDBResource]] = {
    val dbIO = NIHDB.open(chef, baseDir, cookThreshold, storageTimeout, txLogScheduler)(actorSystem)
    dbIO map (_ map { dbV =>
      val resV = dbV map (NIHDBResource(_)(actorSystem.dispatcher))
      fromExtractorError("Failed to open NIHDB") <-: resV :-> (_._2)
    })
  }

  final val blobMetadataFilename = "blob_metadata")

  def isBlob(versionDir: File): Boolean = (new File(versionDir, blobMetadataFilename)).exists

  /**
   * Open the blob for reading in `baseDir`.
   */
  def openBlob(versionDir: File): IO[Validation[ResourceError, Blob]] = IO {
    val metadataStore = PersistentJValue(versionDir, blobMetadataFilename)
    val metadata = metadataStore.validated[BlobMetadata]
    val resource = metadata map { metadata =>
      Blob(metadata, new File(versionDir, "data"))(actorSystem.dispatcher)
    }
    fromExtractorError("Error reading metadata") <-: resource
  }

  /**
   * Creates a blob from a data stream.
   */
  def createBlob[M[+_]: Monad](versionDir: File, mimeType: MimeType, authorities: Authorities,
      data: StreamT[M, Array[Byte]]): M[Validation[ResourceError, Blob]] = {

    val file = new File(versionDir, "data")
    val out = new FileOutputStream(file)
    def write(size: Long, stream: StreamT[M, Array[Byte]]): M[Validation[ResourceError, Long]] = {
      stream.uncons flatMap {
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
        Success(Blob(metadata, file)(actorSystem.dispatcher))
      } catch { case (ioe: IOException) =>
        Failure(IOError(ioe))
      }
    })
  }
}
