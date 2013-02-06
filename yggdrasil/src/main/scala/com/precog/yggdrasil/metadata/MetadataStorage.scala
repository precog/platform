package com.precog.yggdrasil
package metadata

import com.precog.common.json._
import com.precog.util._
import com.precog.common._

import com.weiglewilczek.slf4s.Logging

import blueeyes.json._
import blueeyes.json.serialization._
import blueeyes.json.serialization.DefaultSerialization._
import blueeyes.json.serialization.Extractor._

import java.io.{File, FileReader, FileWriter}
import java.util.concurrent.ConcurrentHashMap

import scalaz.{Validation, Success, Failure}
import scalaz.effect._
import scalaz.std.option._
import scalaz.std.set._
import scalaz.syntax.apply._
import scalaz.syntax.bifunctor._
import scalaz.syntax.show._
import scalaz.syntax.semigroup._
import scalaz.syntax.traverse._
import scalaz.syntax.std.option._
import scalaz.std.map._

import scala.collection.GenTraversableOnce
import scala.collection.JavaConverters._
import scala.collection.mutable.ConcurrentMap

object MetadataStorage {
  case class ResolvedSelector(selector: CPath, authorities: Authorities, descriptor: ProjectionDescriptor, metadata: ColumnMetadata) {
    def columnType: CType = descriptor.columns.find(_.selector == selector).map(_.valueType).get
  }
}

trait MetadataStorage {
  import MetadataStorage._

  def ensureDescriptorRoot(desc: ProjectionDescriptor): IO[File]
  def findDescriptorRoot(desc: ProjectionDescriptor): Option[File]
  def findArchiveRoot(desc: ProjectionDescriptor): IO[Option[File]]

  def findDescriptors(f: ProjectionDescriptor => Boolean): Set[ProjectionDescriptor]

  def getMetadata(desc: ProjectionDescriptor): IO[MetadataRecord] 
  def updateMetadata(desc: ProjectionDescriptor, metadata: MetadataRecord): IO[PrecogUnit]
  def archiveMetadata(desc: ProjectionDescriptor): IO[PrecogUnit]

  def findChildren(path: Path): Set[Path] =
    findDescriptors(_ => true) flatMap { descriptor => 
      descriptor.columns.collect { 
        case ColumnDescriptor(cpath, cselector, _, _) if cpath isChildOf path => {
          Path(cpath.elements(path.length))
        }
      }
    }

  def findSelectors(path: Path): Set[CPath] = 
    findDescriptors(_ => true) flatMap { descriptor =>
      descriptor.columns.collect { 
        case ColumnDescriptor(cpath, cselector, _, _) if path == cpath => cselector 
      }
    }

  def findPathMetadata(path: Path, selector: CPath, columnMetadata: ProjectionDescriptor => IO[ColumnMetadata] = getMetadata(_: ProjectionDescriptor).map(_.metadata)): IO[PathRoot] = {
    @inline def isLeaf(ref: CPath, test: CPath) = {
      (test.nodes startsWith ref.nodes) && 
      test.nodes.length - 1 == ref.nodes.length
    }
    
    @inline def isObjectBranch(ref: CPath, test: CPath) = {
      (test.nodes startsWith ref.nodes) && 
      test.nodes.length > ref.nodes.length &&
      (test.nodes(ref.nodes.length) match {
        case CPathField(_) => true
        case _             => false
      })
    }
    
    @inline def isArrayBranch(ref: CPath, test: CPath) = {
      (test.nodes startsWith ref.nodes) && 
      test.nodes.length > ref.nodes.length &&
      (test.nodes(ref.nodes.length) match {
        case CPathIndex(_) => true
        case _             => false
      })
    }

    def extractIndex(base: CPath, child: CPath): Int = child.nodes(base.length) match {
      case CPathIndex(i) => i
      case _             => sys.error("assertion failed") 
    }

    def extractName(base: CPath, child: CPath): String = child.nodes(base.length) match {
      case CPathField(n) => n 
      case _             => sys.error("unpossible")
    }

    def newIsLeaf(ref: CPath, test: CPath): Boolean = ref == test 

    def selectorPartition(sel: CPath, rss: Set[ResolvedSelector]): (Set[ResolvedSelector], Set[ResolvedSelector], Set[Int], Set[String]) = {
      val (values, nonValues) = rss.partition(rs => newIsLeaf(sel, rs.selector))
      val (indexes, fields) = rss.foldLeft( (Set.empty[Int], Set.empty[String]) ) {
        case (acc @ (is, fs), rs) => if(isArrayBranch(sel, rs.selector)) {
          (is + extractIndex(sel, rs.selector), fs)
        } else if(isObjectBranch(sel, rs.selector)) {
          (is, fs + extractName(sel, rs.selector))
        } else {
          acc
        }
      }

      (values, nonValues, indexes, fields)
    }

    def convertValues(values: Set[ResolvedSelector]): Set[PathMetadata] = {
      values.foldLeft(Map[(CPath, CType), (Authorities, Map[ProjectionDescriptor, ColumnMetadata])]()) {
        case (acc, rs @ ResolvedSelector(sel, auth, desc, meta)) => 
          val key = (sel, rs.columnType)
          val update = acc.get(key).map(_._2).getOrElse( Map.empty[ProjectionDescriptor, ColumnMetadata] ) + (desc -> meta)
          acc + (key -> (auth, update))
      }.map {
        case ((sel, colType), (auth, meta)) => PathValue(colType, auth, meta) 
      }(collection.breakOut)
    }

    def buildTree(branch: CPath, rs: Set[ResolvedSelector]): Set[PathMetadata] = {
      val (values, nonValues, indexes, fields) = selectorPartition(branch, rs)

      val oval = convertValues(values)
      val oidx = indexes.map { idx => PathIndex(idx, buildTree(branch \ idx, nonValues)) }
      val ofld = fields.map { fld => PathField(fld, buildTree(branch \ fld, nonValues)) }

      oval ++ oidx ++ ofld
    }

    val matching: Set[IO[ResolvedSelector]] = findDescriptors {
      descriptor => descriptor.columns.exists { _.isChildOf(path, selector) }
    } flatMap { descriptor => 
      descriptor.columns.collect {
        case cd @ ColumnDescriptor(cpath, csel, _, auth) if cd.isChildOf(path, selector) =>
          columnMetadata(descriptor) map { cm => ResolvedSelector(csel, auth, descriptor, cm) }
      }
    }

    matching.sequence[IO, ResolvedSelector].map(s => PathRoot(buildTree(selector, s)))
  }
}

object FileMetadataStorage extends Logging {
  final val descriptorName = "projection_descriptor.json"
  final val prevFilename = "projection_metadata.prev"
  final val curFilename = "projection_metadata.cur"
  final val nextFilename = "projection_metadata.next"

  def load(baseDir: File, archiveDir: File, fileOps: FileOps): IO[FileMetadataStorage] = {
    val checkBaseDir = IO {
      if (!baseDir.isDirectory) throw new IllegalArgumentException("FileMetadataStorage cannot use non-directory %s for its base".format(baseDir))
      if (!baseDir.canRead) throw new IllegalArgumentException("FileMetadataStorage cannot read base directory " + baseDir)
      if (!baseDir.canWrite) throw new IllegalArgumentException("FileMetadataStorage cannot write base directory " + baseDir)
      
      // Don't require the existence of the archiveDir, just ensure that it can be created.
      val archiveParentDir = archiveDir.getParentFile
      if (!archiveParentDir.isDirectory) throw new IllegalArgumentException("FileMetadataStorage cannot use non-directory %s for its archive".format(archiveDir))
      if (!archiveParentDir.canRead) throw new IllegalArgumentException("FileMetadataStorage cannot read archive directory " + archiveDir)
      if (!archiveParentDir.canWrite) throw new IllegalArgumentException("FileMetadataStorage cannot write archive directory " + archiveDir)
    }

    for {
      _ <- checkBaseDir
      locations <- loadDescriptors(baseDir)
    } yield {
      new FileMetadataStorage(baseDir, archiveDir, fileOps, locations)
    }
  }

  private def loadDescriptors(baseDir: File): IO[ConcurrentMap[ProjectionDescriptor, File]] = {
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
      val initialMap = (new ConcurrentHashMap[ProjectionDescriptor, File]).asScala
      walkDirs(baseDir).foldLeft(initialMap) { (acc, dir) =>
        logger.debug("loading: " + dir)
        read(dir) match {
          case Success(pd) => 
            logger.debug("Loaded projection descriptor " + pd.shows + " from " + dir.toString)
            acc += (pd -> dir)
          case Failure(error) => 
            logger.warn("Failed to restore %s: %s".format(dir, error))
            acc
        }
      }
    }

    def read(baseDir: File): Validation[String, ProjectionDescriptor] = {
      val onError = (ex: Throwable) => {
        logger.error("Failure parsing serialized projection descriptor", ex) 
        "An error occurred parsing serialized projection descriptor: " + ex.getMessage
      }

      val df = new File(baseDir, descriptorName)
      if (!df.exists) {
        Failure("Unable to find serialized projection descriptor in " + baseDir)
      } else {
        ((_: Extractor.Error).message) <-: (for {
          jv <- ((Extractor.Invalid(_: String)) compose onError) <-: JParser.parseFromFile(df)
          desc <- jv.validated[ProjectionDescriptor]
        } yield desc)
      }
    }

    IO(loadMap(baseDir))
  }

  private def defaultMetadata(desc: ProjectionDescriptor): MetadataRecord = {
    val metadata: ColumnMetadata = desc.columns.map { col => (col -> Map.empty[MetadataType, Metadata]) }(collection.breakOut)
    MetadataRecord(metadata, VectorClock.empty)
  }
}

class FileMetadataStorage private (baseDir: File, archiveDir: File, fileOps: FileOps, private val metadataLocations: ConcurrentMap[ProjectionDescriptor, File]) extends MetadataStorage with Logging {
  import FileMetadataStorage._
  def findDescriptors(f: ProjectionDescriptor => Boolean): Set[ProjectionDescriptor] = {
    metadataLocations.keySet.filter(f).toSet
  }

  def ensureDescriptorRoot(desc: ProjectionDescriptor): IO[File] = {
    if (metadataLocations.contains(desc)) {
      IO(metadataLocations(desc))
    } else {
      val newRoot = descriptorDir(baseDir, desc)
      for {
        _       <- IO { newRoot.mkdirs() }
        _       <- writeDescriptor(desc, newRoot) 
      } yield {
        logger.info("Created new projection for " + desc)
        metadataLocations += (desc -> newRoot) 
        newRoot
      }
    }
  }

  def findDescriptorRoot(desc: ProjectionDescriptor): Option[File] = metadataLocations.get(desc)

  def findArchiveRoot(desc: ProjectionDescriptor): IO[Option[File]] = IO {
    metadataLocations.get(desc) map { _ => 
      val dir = descriptorDir(archiveDir, desc)
      dir.mkdirs()
      dir
    }
  }

  def getMetadata(desc: ProjectionDescriptor): IO[MetadataRecord] = {
    import MetadataRecord._
    implicit val extractor = metadataRecordExtractor(desc)

    metadataLocations.get(desc) match {
      case Some(dir) =>
        val file = new File(dir, curFilename)
        fileOps.exists(file) flatMap {
          case true  => fileOps.read(file) map { json => JParser.parseUnsafe(json).deserialize[MetadataRecord] }
          case false => IO(defaultMetadata(desc))
        }

      case None =>
        IO(defaultMetadata(desc))
    }
  }
 
  def updateMetadata(desc: ProjectionDescriptor, metadata: MetadataRecord): IO[PrecogUnit] = {
    logger.debug("Updating metadata for " + desc)
    metadataLocations.get(desc) map { dir =>
      metadataLocations += (desc -> dir)

      for {
        _ <- stageNext(dir, metadata)
        _ <- stagePrev(dir)
        _ <- rotateCurrent(dir)
      } yield {
        logger.debug("Metadata update complete for " + desc)
        PrecogUnit
      }
    } getOrElse {
      IO.throwIO(new IllegalStateException("Metadata update on missing projection for " + desc))
    }
  }
  
  def archiveMetadata(desc: ProjectionDescriptor): IO[PrecogUnit] = IO {
    // Metadata file should already have been moved as a side-effect of archiving
    // the projection, so here we just remove it from the map.
    logger.debug("Archiving metadata for " + desc)
    metadataLocations -= desc
    PrecogUnit
  }

  override def toString = "FileMetadataStorage(root = " + baseDir + " archive = " + archiveDir +")"

  private final val disallowedPathComponents = Set(".", "..")
  /**
   * Computes the stable path for a given descriptor relative to the given base dir
   */
  private def descriptorDir(baseDir: File, descriptor: ProjectionDescriptor): File = {
    // The path component maps directly to the FS, with a hash on the columnrefs as the final dir
    val prefix = descriptor.commonPrefix.filterNot(disallowedPathComponents.contains)

    new File(baseDir, (prefix :+ descriptor.stableHash).mkString(File.separator))
  }
  
  private def writeDescriptor(desc: ProjectionDescriptor, baseDir: File): IO[PrecogUnit] = {
    val df = new File(baseDir, descriptorName)
    fileOps.exists(df) map {
      case true => 
        throw new java.io.IOException("Serialized projection descriptor already exists for " + desc + " in " + baseDir)

      case false =>
        val writer = new FileWriter(df)
        try {
          writer.write(desc.serialize.renderPretty)
          PrecogUnit
        } finally {
          writer.close()
        }
    }
  }

  private def stageNext(dir: File, metadata: MetadataRecord): IO[PrecogUnit] = {
    val json = metadata.serialize.renderPretty
    val next = new File(dir, nextFilename)
    fileOps.write(next, json)
  }
  
  private def stagePrev(dir: File): IO[PrecogUnit] = {
    val src = new File(dir, curFilename)
    val dest = new File(dir, prevFilename)
    fileOps.exists(src) flatMap { exists => if (exists) fileOps.copy(src, dest) else IO(PrecogUnit) }
  }
  
  private def rotateCurrent(dir: File): IO[PrecogUnit] = {
    val src  = new File(dir, nextFilename)
    val dest = new File(dir, curFilename)
    for {
      _ <- IO { logger.trace("Rotating metadata from %s to %s".format(nextFilename, curFilename)) }
      _ <- fileOps.rename(src, dest)
    } yield PrecogUnit
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

