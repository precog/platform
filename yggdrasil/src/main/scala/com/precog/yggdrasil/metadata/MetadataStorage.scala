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

import blueeyes.json.Printer
import blueeyes.json.JsonParser
import blueeyes.json.JsonAST._
import blueeyes.json.xschema._
import blueeyes.json.xschema.DefaultSerialization._
import blueeyes.json.xschema.Extractor._

import java.io.File
import scalaz.{Validation, Success, Failure}
import scalaz.effect._
import scalaz.syntax.apply._

object MetadataStorage {
  val prevFilename = "projection_metadata.prev"
  val curFilename = "projection_metadata.cur"
  val nextFilename = "projection_metadata.next"
}

trait MetadataStorage {
  import MetadataStorage._

  val fileOps: FileOps

  protected def dirMapping: ProjectionDescriptor => IO[File]

  def currentMetadata(desc: ProjectionDescriptor): IO[Validation[Error, MetadataRecord]] = {
    def extract(json: String): Validation[Error, MetadataRecord] = 
      JsonParser.parse(json).validated[PartialMetadataRecord].map { _.toMetadataRecord(desc) }

    dirMapping(desc) flatMap { dir =>
      val file = new File(dir, curFilename)
      fileOps.read(file) map { opt =>
        opt.map { extract } getOrElse { Success(defaultMetadata(desc)) }
      }
    }
  }
 
  private def defaultMetadata(desc: ProjectionDescriptor): MetadataRecord = {
    val metadata: ColumnMetadata = desc.columns.map { col =>
      (col -> Map.empty[MetadataType, Metadata])
    }(collection.breakOut)
    MetadataRecord(metadata, VectorClock.empty)
  }
  
  def updateMetadata(desc: ProjectionDescriptor, metadata: MetadataRecord): IO[Validation[Throwable, Unit]] = 
    dirMapping(desc) flatMap { dir => 
      stageNext(dir, metadata) flatMap { 
        case Success(_) => 
          stagePrev(dir) flatMap {
            case Success(_)     => rotateCurrent(dir)
            case f @ Failure(_) => IO(f)
          }
        case f @ Failure(_) => IO(f)
      }
    }

  def stageNext(dir: File, metadata: MetadataRecord): IO[Validation[Throwable, Unit]] = {
    val json = Printer.pretty(Printer.render(metadata.serialize))
    val next = new File(dir, nextFilename)
    fileOps.write(next, json)
  }
  
  def stagePrev(dir: File): IO[Validation[Throwable, Unit]] = {
    val src = new File(dir, curFilename)
    val dest = new File(dir, prevFilename)
    if (fileOps.exists(src)) fileOps.copy(src, dest) else IO{ Success(()) }
  }
  
  def rotateCurrent(dir: File): IO[Validation[Throwable, Unit]] = IO {
    Validation.fromTryCatch {
      val src = new File(dir, nextFilename)
      val dest = new File(dir, curFilename)
      fileOps.rename(src, dest)
    }
  }
}


class FilesystemMetadataStorage(protected val dirMapping: ProjectionDescriptor => IO[File]) extends MetadataStorage {
  object fileOps extends FilesystemFileOps
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
}

object MetadataRecord extends MetadataRecordSerialization


case class PartialMetadataRecord(metadata: List[MetadataMap], clock: VectorClock) {
  def toMetadataRecord(desc: ProjectionDescriptor): MetadataRecord = {
    val map: Map[ColumnDescriptor, MetadataMap] = (desc.columns zip metadata)(collection.breakOut)
    MetadataRecord(map, clock)   
  } 
}

trait PartialMetadataRecordSerialization {
  private def extractMetadata(v: Validation[Error, List[Set[Metadata]]]): Validation[Error, List[MetadataMap]] = {
    v.map { _.map( sm => Metadata.toTypedMap(sm) ).toList }
  }

  implicit val MetadataRecordExtractor: Extractor[PartialMetadataRecord] = new Extractor[PartialMetadataRecord] with ValidatedExtraction[PartialMetadataRecord] {
    override def validated(obj: JValue): Validation[Error, PartialMetadataRecord] =
      (extractMetadata((obj \ "metadata").validated[List[Set[Metadata]]]) |@|
       (obj \ "checkpoint").validated[VectorClock]).apply(PartialMetadataRecord(_, _))
  }
}

object PartialMetadataRecord extends PartialMetadataRecordSerialization
// vim: set ts=4 sw=4 et:
