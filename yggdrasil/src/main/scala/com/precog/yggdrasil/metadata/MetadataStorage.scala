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

import blueeyes.json.Printer
import blueeyes.json.JsonParser
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import java.io.{File, FileReader}
import scalaz.{Validation, Success, Failure}
import scalaz.effect._
import scalaz.syntax.apply._
import scalaz.syntax.semigroup._
import scalaz.std.map._

import scala.collection.GenTraversableOnce

trait MetadataStorage {
  def findDescriptorRoot(desc: ProjectionDescriptor): Option[File]
  def findDescriptors(f: ProjectionDescriptor => Boolean): Set[ProjectionDescriptor]
  def flatMapDescriptors[T](f: ProjectionDescriptor => GenTraversableOnce[T]): Seq[T]
  def currentMetadata(desc: ProjectionDescriptor): IO[Validation[Error, MetadataRecord]] 
  def updateMetadata(desc: ProjectionDescriptor, metadata: MetadataRecord): IO[Validation[Throwable, Unit]] 
}

object FileMetadataStorage {
  final val descriptorName = "projection_descriptor.json"
  final val prevFilename = "projection_metadata.prev"
  final val curFilename = "projection_metadata.cur"
  final val nextFilename = "projection_metadata.next"
}

class FileMetadataStorage(baseDir: File, fileOps: FileOps) extends MetadataStorage with Logging {
  import FileMetadataStorage._

  private var metadataLocations: Map[ProjectionDescriptor, File] = loadDescriptors(baseDir)

  def findDescriptorRoot(desc: ProjectionDescriptor): Option[File] = metadataLocations.get(desc)

  def findDescriptors(f: ProjectionDescriptor => Boolean): Set[ProjectionDescriptor] = {
    metadataLocations.keySet.filter(f)
  }

  def flatMapDescriptors[T](f: ProjectionDescriptor => GenTraversableOnce[T]): Seq[T] = {
    metadataLocations.keySet.toList.flatMap(f)
  }

  def currentMetadata(desc: ProjectionDescriptor): IO[Validation[Error, MetadataRecord]] = {
    import MetadataRecord._
    implicit val extractor = metadataRecordExtractor(desc)

    metadataLocations.get(desc) match {
      case Some(dir) =>
        val file = new File(dir, curFilename)
        fileOps.read(file) map { opt =>
          opt.map { json => JsonParser.parse(json).validated[MetadataRecord] } getOrElse { Success(defaultMetadata(desc)) }
        }

      case None =>
        IO(Success(defaultMetadata(desc)))
    }
  }
 
  def updateMetadata(desc: ProjectionDescriptor, metadata: MetadataRecord): IO[Validation[Throwable, Unit]] = {
    val dir = metadataLocations.getOrElse(desc, newRandomDir(baseDir))
    metadataLocations += (desc -> dir)

    stageNext(dir, metadata) flatMap { 
      case Success(_) => 
        stagePrev(dir) flatMap {
          case Success(_)     => rotateCurrent(dir)
          case f @ Failure(_) => IO(f)
        }
      case f @ Failure(_) => IO(f)
    }
  }

  private def newRandomDir(parent: File): File = {
    def dirUUID: String = {
      val uuid = java.util.UUID.randomUUID.toString.toLowerCase.replace("-", "")
      val randomPath = (1 until 3).map { _*2 }.foldLeft(Vector.empty[String]) {
        case (acc, i) => acc :+ uuid.substring(0, i)
      }
      
      randomPath.mkString("/", "/", "/") + uuid
    }

    val newDir = new File(parent, dirUUID)
    newDir.mkdirs
    newDir
  }

  private def walkDirs(baseDir: File): Seq[File] = {
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
      Seq()
    }
  }

  private def loadDescriptors(baseDir: File): Map[ProjectionDescriptor, File] = {
    def loadMap(baseDir: File) = {
      walkDirs(baseDir).foldLeft(Map.empty[ProjectionDescriptor, File]) { (acc, dir) =>
        logger.debug("loading: " + dir)
        read(dir) match {
          case Success(pd) => acc + (pd -> dir)
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

    loadMap(baseDir)
  }

  private def defaultMetadata(desc: ProjectionDescriptor): MetadataRecord = {
    val metadata: ColumnMetadata = desc.columns.map { col =>
      (col -> Map.empty[MetadataType, Metadata])
    }(collection.breakOut)

    MetadataRecord(metadata, VectorClock.empty)
  }

  private def stageNext(dir: File, metadata: MetadataRecord): IO[Validation[Throwable, Unit]] = {
    val json = Printer.pretty(Printer.render(metadata.serialize))
    val next = new File(dir, nextFilename)
    fileOps.write(next, json)
  }
  
  private def stagePrev(dir: File): IO[Validation[Throwable, Unit]] = {
    val src = new File(dir, curFilename)
    val dest = new File(dir, prevFilename)
    if (fileOps.exists(src)) fileOps.copy(src, dest) else IO{ Success(()) }
  }
  
  private def rotateCurrent(dir: File): IO[Validation[Throwable, Unit]] = IO {
    Validation.fromTryCatch {
      val src = new File(dir, nextFilename)
      val dest = new File(dir, curFilename)
      fileOps.rename(src, dest)
    }
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
