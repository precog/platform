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
package metadata

import com.precog.util._
import com.precog.common._

import com.weiglewilczek.slf4s.Logging

import blueeyes.json._
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import java.io.{File, FileReader, FileWriter}
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

object MetadataStorage {
  case class ResolvedSelector(selector: JPath, authorities: Authorities, descriptor: ProjectionDescriptor, metadata: ColumnMetadata) {
    def columnType: CType = descriptor.columns.find(_.selector == selector).map(_.valueType).get
  }
}

trait MetadataStorage {
  import MetadataStorage._

  def findDescriptorRoot(desc: ProjectionDescriptor, createOk: Boolean): IO[Option[File]]
  def findArchiveRoot(desc: ProjectionDescriptor): IO[Option[File]]
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

  def findPathMetadata(path: Path, selector: JPath, columnMetadata: ProjectionDescriptor => IO[ColumnMetadata] = getMetadata(_: ProjectionDescriptor).map(_.metadata)): IO[PathRoot] = {
    @inline def isLeaf(ref: JPath, test: JPath) = {
      (test.nodes startsWith ref.nodes) && 
      test.nodes.length - 1 == ref.nodes.length
    }
    
    @inline def isObjectBranch(ref: JPath, test: JPath) = {
      (test.nodes startsWith ref.nodes) && 
      test.nodes.length > ref.nodes.length &&
      (test.nodes(ref.nodes.length) match {
        case JPathField(_) => true
        case _             => false
      })
    }
    
    @inline def isArrayBranch(ref: JPath, test: JPath) = {
      (test.nodes startsWith ref.nodes) && 
      test.nodes.length > ref.nodes.length &&
      (test.nodes(ref.nodes.length) match {
        case JPathIndex(_) => true
        case _             => false
      })
    }

    def extractIndex(base: JPath, child: JPath): Int = child.nodes(base.length) match {
      case JPathIndex(i) => i
      case _             => sys.error("assertion failed") 
    }

    def extractName(base: JPath, child: JPath): String = child.nodes(base.length) match {
      case JPathField(n) => n 
      case _             => sys.error("unpossible")
    }

    def newIsLeaf(ref: JPath, test: JPath): Boolean = ref == test 

    def selectorPartition(sel: JPath, rss: Set[ResolvedSelector]): (Set[ResolvedSelector], Set[ResolvedSelector], Set[Int], Set[String]) = {
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
      values.foldLeft(Map[(JPath, CType), (Authorities, Map[ProjectionDescriptor, ColumnMetadata])]()) {
        case (acc, rs @ ResolvedSelector(sel, auth, desc, meta)) => 
          val key = (sel, rs.columnType)
          val update = acc.get(key).map(_._2).getOrElse( Map.empty[ProjectionDescriptor, ColumnMetadata] ) + (desc -> meta)
          acc + (key -> (auth, update))
      }.map {
        case ((sel, colType), (auth, meta)) => PathValue(colType, auth, meta) 
      }(collection.breakOut)
    }

    def buildTree(branch: JPath, rs: Set[ResolvedSelector]): Set[PathMetadata] = {
      val (values, nonValues, indexes, fields) = selectorPartition(branch, rs)

      val oval = convertValues(values)
      val oidx = indexes.map { idx => PathIndex(idx, buildTree(branch \ idx, nonValues)) }
      val ofld = fields.map { fld => PathField(fld, buildTree(branch \ fld, nonValues)) }

      oval ++ oidx ++ ofld
    }

    val matching: Set[IO[ResolvedSelector]] = findDescriptors(_ => true) flatMap { descriptor => 
      descriptor.columns.collect {
        case ColumnDescriptor(cpath, csel, _, auth) if cpath == path && (csel.nodes startsWith selector.nodes) =>
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
    for {
      _  <- IO {
              if (!baseDir.isDirectory) throw new IllegalArgumentException("FileMetadataStorage cannot use non-directory %s for its base".format(baseDir))
              if (!baseDir.canRead) throw new IllegalArgumentException("FileMetadataStorage cannot read base directory " + baseDir)
              if (!baseDir.canWrite) throw new IllegalArgumentException("FileMetadataStorage cannot write base directory " + baseDir)
              
              // Don't require the existence of the archiveDir, just ensure that it can be created.
              val archiveParentDir = archiveDir.getParentFile
              if (!archiveParentDir.isDirectory) throw new IllegalArgumentException("FileMetadataStorage cannot use non-directory %s for its archive".format(archiveDir))
              if (!archiveParentDir.canRead) throw new IllegalArgumentException("FileMetadataStorage cannot read archive directory " + archiveDir)
              if (!archiveParentDir.canWrite) throw new IllegalArgumentException("FileMetadataStorage cannot write archive directory " + archiveDir)
            }
      locations <- loadDescriptors(baseDir)
    } yield {
      new FileMetadataStorage(baseDir, archiveDir, fileOps, locations)
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
          { (_: Extractor.Error).message } <-: JsonParser.parse(reader).validated[ProjectionDescriptor] 
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

class FileMetadataStorage(baseDir: File, archiveDir: File, fileOps: FileOps, private var metadataLocations: Map[ProjectionDescriptor, File]) extends MetadataStorage with Logging {
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

  def findArchiveRoot(desc: ProjectionDescriptor): IO[Option[File]] = {
    metadataLocations.get(desc).map(newArchiveDir(archiveDir, _)).sequence
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

  override def toString = "FileMetadataStorage(root = " + baseDir + " archive = " + archiveDir +")"

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

  private def newArchiveDir(parent: File, source: File): IO[File] = {
    val dirUUID = Iterator.iterate(source)(_.getParentFile).map(_.getName).take(3).toList.reverse.mkString("/", "/", "")

    val archiveDir = new File(parent, dirUUID)
    IO {
      archiveDir.mkdirs
      archiveDir
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

