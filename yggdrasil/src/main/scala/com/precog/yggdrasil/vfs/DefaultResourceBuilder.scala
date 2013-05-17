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
import com.precog.util.IOUtils

import com.weiglewilczek.slf4s.Logging

import java.io.{IOException, File, FileOutputStream}
import java.util.concurrent.ScheduledThreadPoolExecutor

import scalaz._
import scalaz.NonEmptyList._
import scalaz.\/._
import scalaz.effect.IO
import scalaz.syntax.bifunctor._
import scalaz.syntax.id._
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
  txLogSchedulerSize: Int = 20) extends Logging { // default for now, should come from config in the future

  import Resource._

  private final val txLogScheduler = new ScheduledThreadPoolExecutor(txLogSchedulerSize,
    new ThreadFactoryBuilder().setNameFormat("HOWL-sched-%03d").build())

  private implicit val futureMonad = new FutureMonad(actorSystem.dispatcher)

  private def ensureDescriptorDir(versionDir: File): IO[File] = IO {
    if (versionDir.isDirectory || versionDir.mkdirs) versionDir
    else throw new IOException("Failed to create directory for projection: %s".format(versionDir))
  }

  // Resource creation/open and discovery
  def createNIHDB(versionDir: File, authorities: Authorities): IO[ResourceError \/ NIHDBResource] = {
    for {
      nihDir <- ensureDescriptorDir(versionDir) 
      nihdbV <- NIHDB.create(chef, authorities, nihDir, cookThreshold, storageTimeout, txLogScheduler)(actorSystem)
    } yield { 
      nihdbV.disjunction map {
        NIHDBResource(_, authorities)(actorSystem)
      } leftMap {
        ResourceError.fromExtractorError("Failed to create NIHDB in %s as %s".format(versionDir.toString, authorities)) 
      }
    }
  }

  def openNIHDB(descriptorDir: File): IO[ResourceError \/ NIHDBResource] = {
    NIHDB.open(chef, descriptorDir, cookThreshold, storageTimeout, txLogScheduler)(actorSystem) map {
      _ map { 
        _.disjunction map {
          case (authorities, nihdb) => NIHDBResource(nihdb, authorities)(actorSystem)
        } leftMap {
          ResourceError.fromExtractorError("Failed to open NIHDB from %s".format(descriptorDir.toString)) 
        }
      } getOrElse {
        left(NotFound("No NIHDB projection found in %s".format(descriptorDir)))
      }
    }
  }

  final val blobMetadataFilename = "blob_metadata"

  def isBlob(versionDir: File): Boolean = (new File(versionDir, blobMetadataFilename)).exists

  /**
   * Open the blob for reading in `baseDir`.
   */
  def openBlob(versionDir: File): IO[ResourceError \/ BlobResource] = IO {
    //val metadataStore = PersistentJValue(versionDir, blobMetadataFilename)
    //val metadata = metadataStore.json.validated[BlobMetadata]
    JParser.parseFromFile(new File(versionDir, blobMetadataFilename)).leftMap(Error.thrown).
    flatMap(_.validated[BlobMetadata]).
    disjunction.map { metadata =>
      BlobResource(new File(versionDir, "data"), metadata) //(actorSystem.dispatcher)
    } leftMap {
      ResourceError.fromExtractorError("Error reading metadata from versionDir %s".format(versionDir.toString))
    }
  }

  /**
   * Creates a blob from a data stream.
   */
  def createBlob[M[+_]](versionDir: File, mimeType: MimeType, authorities: Authorities, data: StreamT[M, Array[Byte]])(implicit M: Monad[M], IOT: IO ~> M): M[ResourceError \/ BlobResource] = {
    def write(out: FileOutputStream, size: Long, stream: StreamT[M, Array[Byte]]): M[ResourceError \/ Long] = {
      stream.uncons.flatMap {
        case Some((bytes, tail)) =>
          try {
            out.write(bytes)
            write(out, size + bytes.length, tail)
          } catch { case (ioe: IOException) =>
              out.close()
              left(IOError(ioe)).point[M]
          }

        case None =>
          out.close()
          right(size).point[M]
      }
    }

    for {
      _ <- IOT { IOUtils.makeDirectory(versionDir) }
      file = new File(versionDir, "data") unsafeTap { s => logger.debug("Creating new blob at " + s) }
      writeResult <- write(new FileOutputStream(file), 0L, data)
      blobResult <- IOT { 
        writeResult traverse { size =>
          logger.debug("Write complete on " + file)
          val metadata = BlobMetadata(mimeType, size, clock.now(), authorities)
          //val metadataStore = PersistentJValue(versionDir, blobMetadataFilename)
          //metadataStore.json = metadata.serialize
          IOUtils.writeToFile(metadata.serialize.renderCompact, new File(versionDir, blobMetadataFilename)) map { _ =>
            BlobResource(file, metadata)
          } 
        } 
      }
    } yield blobResult
  }
}
