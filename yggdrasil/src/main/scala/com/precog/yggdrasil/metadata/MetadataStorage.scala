package com.precog.yggdrasil
package metadata

import com.precog.util._
import com.precog.common._

import com.weiglewilczek.slf4s.Logging

import blueeyes.json.Printer
import blueeyes.json.JsonParser
import blueeyes.json.JPath
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import java.io.{File, FileReader, FileWriter}
import scalaz.{Validation, Success, Failure}
import scalaz.effect._
import scalaz.syntax.apply._
import scalaz.syntax.show._
import scalaz.syntax.semigroup._
import scalaz.syntax.std.option._
import scalaz.std.map._

import scala.collection.GenTraversableOnce

trait MetadataStorage {
  def findDescriptorRoot(desc: ProjectionDescriptor, createOk: Boolean): IO[Option[File]]
  def findDescriptors(f: ProjectionDescriptor => Boolean): Set[ProjectionDescriptor]

  def getMetadata(desc: ProjectionDescriptor): IO[MetadataRecord] 
  def updateMetadata(desc: ProjectionDescriptor, metadata: MetadataRecord): IO[Unit]

  def findChildren(path: Path): Set[Path] =
    findDescriptors(_ => true) flatMap { descriptor => 
      descriptor.columns.collect { 
        case ColumnDescriptor(cpath, cselector, _, _) if cpath isChildOf path => {
          Path(cpath.elements(path.length))
        }
      }
    }

  def findSelectors(path: Path): Set[JPath] = 
    findDescriptors(_ => true) flatMap { descriptor =>
      descriptor.columns.collect { 
        case ColumnDescriptor(cpath, cselector, _, _) if path == cpath => cselector 
      }
    }
}

object FileMetadataStorage extends Logging {
  final val descriptorName = "projection_descriptor.json"
  final val prevFilename = "projection_metadata.prev"
  final val curFilename = "projection_metadata.cur"
  final val nextFilename = "projection_metadata.next"

  def load(baseDir: File, fileOps: FileOps): IO[FileMetadataStorage] = {
    for {
      _  <- IO {
              if (!baseDir.isDirectory) throw new IllegalArgumentException("FileMetadataStorage cannot use non-directory %s for its base".format(baseDir))
              if (!baseDir.canRead) throw new IllegalArgumentException("FileMetadataStorage cannot read base directory " + baseDir)
            }
      locations <- loadDescriptors(baseDir)
    } yield {
      new FileMetadataStorage(baseDir, fileOps, locations)
    }
  }

  private def loadDescriptors(baseDir: File): IO[Map[ProjectionDescriptor, File]] = {
    def walkDirs(baseDir: File): Seq[File] = {
      def containsDescriptor(dir: File) = new File(dir, descriptorName).isFile 

      def walk(baseDir: File): Seq[File] = {
        if(containsDescriptor(baseDir)) {
          Vector(baseDir)
        } else {
          baseDir.listFiles.filter(_.isDirectory).flatMap{ walk(_) }
        }
      }

      if (baseDir.isDirectory) {
        walk(baseDir)
      } else {
        logger.warn("Base dir is not a directory!!!")
        Seq()
      }
    }


    def loadMap(baseDir: File) = {
      walkDirs(baseDir).foldLeft(Map.empty[ProjectionDescriptor, File]) { (acc, dir) =>
        logger.debug("loading: " + dir)
        read(dir) match {
          case Success(pd) => 
            logger.debug("Loaded projection descriptor " + pd.shows + " from " + dir.toString)
            acc + (pd -> dir)
          case Failure(error) => 
            logger.warn("Failed to restore %s: %s".format(dir, error))
            acc
        }
      }
    }

    def read(baseDir: File): Validation[String, ProjectionDescriptor] = {
      val df = new File(baseDir, descriptorName)
      if (!df.exists) Failure("Unable to find serialized projection descriptor in " + baseDir)
      else {
        val reader = new FileReader(df)
        try {
          // TODO: scalaz eludes me... (DCB)
          //{ (err: Extractor.Error) => err.message } <-: JsonParser.parse(reader).validated[ProjectionDescriptor]
          JsonParser.parse(reader).validated[ProjectionDescriptor].fail.map { (err: Extractor.Error) => err.message } validation
        } finally {
          reader.close
        }
      }
    }

    IO(loadMap(baseDir))
  }

  private def defaultMetadata(desc: ProjectionDescriptor): MetadataRecord = {
    val metadata: ColumnMetadata = desc.columns.map { col => (col -> Map.empty[MetadataType, Metadata]) }(collection.breakOut)
    MetadataRecord(metadata, VectorClock.empty)
  }
}

class FileMetadataStorage(baseDir: File, fileOps: FileOps, private var metadataLocations: Map[ProjectionDescriptor, File]) extends MetadataStorage with Logging {
  import FileMetadataStorage._
  def findDescriptors(f: ProjectionDescriptor => Boolean): Set[ProjectionDescriptor] = {
    metadataLocations.keySet.filter(f)
  }

  def ensureDescriptorRoot(desc: ProjectionDescriptor): IO[Unit] = {
    if (metadataLocations.contains(desc)) {
      IO(())
    } else {
      for {
        newRoot <- newRandomDir(baseDir)
        _       <- writeDescriptor(desc, newRoot) 
      } yield {
        logger.info("Created new projection for " + desc)
        metadataLocations += (desc -> newRoot) 
      }
    }
  }

  def findDescriptorRoot(desc: ProjectionDescriptor, createOk: Boolean): IO[Option[File]] = {
    if (createOk) {
      for (_ <- ensureDescriptorRoot(desc)) yield metadataLocations.get(desc)
    } else {
      IO(metadataLocations.get(desc))
    }
  }

  def getMetadata(desc: ProjectionDescriptor): IO[MetadataRecord] = {
    import MetadataRecord._
    implicit val extractor = metadataRecordExtractor(desc)

    metadataLocations.get(desc) match {
      case Some(dir) =>
        val file = new File(dir, curFilename)
        fileOps.exists(file) flatMap {
          case true  => fileOps.read(file) map { json => JsonParser.parse(json).deserialize[MetadataRecord] }
          case false => IO(defaultMetadata(desc))
        }

      case None =>
        IO(defaultMetadata(desc))
    }
  }
 
  def updateMetadata(desc: ProjectionDescriptor, metadata: MetadataRecord): IO[Unit] = {
    logger.debug("Updating metadata for " + desc)
    metadataLocations.get(desc) map { dir =>
      metadataLocations += (desc -> dir)

      for {
        _ <- stageNext(dir, metadata)
        _ <- stagePrev(dir)
        _ <- rotateCurrent(dir)
      } yield ()
    } getOrElse {
      IO.throwIO(new IllegalStateException("Metadata update on missing projection for " + desc))
    }
  }

  private def newRandomDir(parent: File): IO[File] = {
    def dirUUID: String = {
      val uuid = java.util.UUID.randomUUID.toString.toLowerCase.replace("-", "")
      val randomPath = (1 until 3).map { _*2 }.foldLeft(Vector.empty[String]) {
        case (acc, i) => acc :+ uuid.substring(0, i)
      }
      
      randomPath.mkString("/", "/", "/") + uuid
    }

    val newDir = new File(parent, dirUUID)
    IO {
      newDir.mkdirs
      newDir
    }
  }

  private def writeDescriptor(desc: ProjectionDescriptor, baseDir: File): IO[Unit] = {
    val df = new File(baseDir, descriptorName)
    fileOps.exists(df) map {
      case true => 
        throw new java.io.IOException("Serialized projection descriptor already exists for " + desc + " in " + baseDir)

      case false =>
        val writer = new FileWriter(df)
        try {
          writer.write(Printer.pretty(Printer.render(desc.serialize)))
        } finally {
          writer.close()
        }
    }
  }

  private def stageNext(dir: File, metadata: MetadataRecord): IO[Unit] = {
    val json = Printer.pretty(Printer.render(metadata.serialize))
    val next = new File(dir, nextFilename)
    fileOps.write(next, json)
  }
  
  private def stagePrev(dir: File): IO[Unit] = {
    val src = new File(dir, curFilename)
    val dest = new File(dir, prevFilename)
    fileOps.exists(src) flatMap { exists => if (exists) fileOps.copy(src, dest) else IO(()) }
  }
  
  private def rotateCurrent(dir: File): IO[Unit] = IO {
    val src  = new File(dir, nextFilename)
    val dest = new File(dir, curFilename)
    fileOps.rename(src, dest)
  }
}

case class MetadataRecord(metadata: ColumnMetadata, clock: VectorClock)

trait MetadataRecordSerialization {
  implicit val MetadataRecordDecomposer: Decomposer[MetadataRecord] = new Decomposer[MetadataRecord] {
    def extract(metadata: ColumnMetadata) = {
      val list: List[Set[Metadata]] = metadata.values.map{ _.values.toSet }(collection.breakOut)
      list.serialize
    }
    override def decompose(metadata: MetadataRecord): JValue = JObject(List(
      JField("metadata", extract(metadata.metadata)),
      JField("checkpoint", metadata.clock)
    ))
  }

  private def extractMetadata(v: Validation[Error, List[Set[Metadata]]]): Validation[Error, List[MetadataMap]] = {
    v.map { _.map( sm => Metadata.toTypedMap(sm) ).toList }
  }

  def metadataRecordExtractor(desc: ProjectionDescriptor): Extractor[MetadataRecord] = new Extractor[MetadataRecord] with ValidatedExtraction[MetadataRecord] {
    override def validated(obj: JValue): Validation[Error, MetadataRecord] =
      (extractMetadata((obj \ "metadata").validated[List[Set[Metadata]]]) |@|
       (obj \ "checkpoint").validated[VectorClock]).apply {
         (metadata, clock) => MetadataRecord((desc.columns zip metadata).toMap, clock)
       }
  }
}

object MetadataRecord extends MetadataRecordSerialization

