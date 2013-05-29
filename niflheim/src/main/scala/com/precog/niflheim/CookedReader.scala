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
package com.precog.niflheim

import com.precog.common._

import java.lang.ref.SoftReference

import java.io.{ File, IOException }
import java.io.FileInputStream
import java.nio.channels._

import scalaz._
import scalaz.syntax.traverse._
import scalaz.std.list._

object CookedReader { 
  def load(baseDir: File, metadataFile: File, blockFormat: CookedBlockFormat = VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)), segmentFormat: SegmentFormat = VersionedSegmentFormat(Map(1 -> V1SegmentFormat))): CookedReader = 
    new CookedReader(baseDir, metadataFile, blockFormat, segmentFormat)
}

final class CookedReader(baseDir: File, metadataFile0: File, blockFormat: CookedBlockFormat, segmentFormat: SegmentFormat) extends StorageReader {
  private val metadataFile = if (metadataFile0.isAbsolute) metadataFile0 else new File(baseDir, metadataFile0.getPath)

  private val lock = new AnyRef { }

  def isStable: Boolean = true

  @volatile
  private var block: SoftReference[CookedBlockMetadata] = null

  private def maybeBlock = if (block != null) block.get() else null

  private def read[A](file: File)(f: ReadableByteChannel => A): A = {
    val channel = new FileInputStream(file).getChannel()
    try {
      f(channel)
    } finally {
      channel.close()
    }
  }

  private def loadFromDisk(): Validation[IOException, CookedBlockMetadata] = {
    read(metadataFile) { channel =>
      val segsV = blockFormat.readCookedBlock(channel)
      segsV foreach { segs0 =>
        block = new SoftReference(segs0)
      }
      segsV
    }
  }

  def id: Long = metadata.valueOr(throw _).blockid
  def length: Int = metadata.valueOr(throw _).length

  def snapshot(pathConstraints: Option[Set[CPath]]): Block = {
    val groupedPaths = metadata.valueOr(throw _).segments.groupBy {
      case (segId, _) => segId.cpath
    }

    val refConstraints = pathConstraints map {
      _.flatMap { path =>
        val tpes = groupedPaths.get(path) map {
          _.map { case (segId, _) => segId.ctype }
        } getOrElse {
          Array.empty[CType]
        }

        tpes.map { tpe => ColumnRef(path, tpe) }.toSet
      }
    }

    snapshotRef(refConstraints)
  }

  def snapshotRef(refConstraints: Option[Set[ColumnRef]]): Block = {
    val segments: Seq[Segment] = refConstraints map { refs =>
      load(refs.toList).map({ segs =>
        segs flatMap (_._2)
      }).valueOr { nel => throw nel.head }
    } getOrElse {
      metadata.valueOr(throw _).segments map { case (segId, file0) =>
        val file = if (file0.isAbsolute) file0 else new File(baseDir, file0.getPath)
        read(file) { channel =>
          segmentFormat.reader.readSegment(channel)
        }.valueOr(throw _)
      }
    }

    Block(id, segments, isStable)
  }

  def structure: Iterable[ColumnRef] = metadata.valueOr(throw _).segments map {
    case (segId, _) => ColumnRef(segId.cpath, segId.ctype)
  }

  def metadata: Validation[IOException, CookedBlockMetadata] = {
    val segs = maybeBlock
    if (segs != null) {
      Success(segs)
    } else {
      lock.synchronized {
        val block = maybeBlock
        if (block == null) {
          loadFromDisk()
        } else {
          Success(block)
        }
      }
    }
  }

  private def segmentsByRef: Validation[IOException, Map[ColumnRef, List[File]]] = metadata map { md =>
    md.segments.groupBy(s => (s._1.cpath, s._1.ctype)).map { case ((cpath, ctype), segs) =>
      (ColumnRef(cpath, ctype), segs.map(_._2).toList)
    }.toMap
  }

  def load(paths: List[ColumnRef]): ValidationNel[IOException, List[(ColumnRef, List[Segment])]] = {
    segmentsByRef.toValidationNel flatMap { (segsByRef: Map[ColumnRef, List[File]]) =>
      paths.map { path =>
        val v: ValidationNel[IOException, List[Segment]] = segsByRef.getOrElse(path, Nil).map { file0 =>
          val file = if (file0.isAbsolute) file0 else new File(baseDir, file0.getPath)
          read(file) { channel =>
            segmentFormat.reader.readSegment(channel).toValidationNel
          }
        }.sequence[({ type λ[α] = ValidationNel[IOException, α] })#λ, Segment]
        v map (path -> _)
      }.sequence[({ type λ[α] = ValidationNel[IOException, α] })#λ, (ColumnRef, List[Segment])]
    }
  }
}
