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
